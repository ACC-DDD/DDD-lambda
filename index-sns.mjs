import {SNS} from "@aws-sdk/client-sns";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {QueryCommand, DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";

const sns = new SNS({region: "ap-northeast-1"});
const dynamoClient = new DynamoDBClient({region: "ap-northeast-2"});
const ddbDocClient = DynamoDBDocumentClient.from(dynamoClient);

const getPhoneNumbersByRegionFromDynamoDB = async (targetRegion) => {
  let phoneNumbers = [];
  const tableName = "Member";
  const gsiName = "address-index";

  let ExclusiveStartKey = undefined; // GSI 조건 및 설정

  do {
    const command = new QueryCommand({
      TableName: tableName,
      IndexName: gsiName,
      KeyConditionExpression: "#addressName = :targetAddressValue",
      ExpressionAttributeNames: {
        "#addressName": "address",
      },
      ExpressionAttributeValues: {
        ":targetAddressValue": targetRegion,
      },
      ProjectionExpression: "phoneNum",
      ExclusiveStartKey,
    });

    const result = await ddbDocClient.send(command);
    result.Items?.forEach((item) => {
      if (item.phoneNum) {
        phoneNumbers.push(item.phoneNum);
      }
    });

    ExclusiveStartKey = result.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return phoneNumbers;
};

const formatPhoneNumberForSns = (phoneNumber) => {
  const digits = phoneNumber.replace(/[^0-9]/g, "");
  if (digits.startsWith("01")) {
    return "+82" + digits.substring(1);
  }
  if (digits.startsWith("82010")) {
    return "+" + digits;
  }
  console.warn(`Invalid phone number format: ${phoneNumber}`);
  return null;
};

export const handler = async (event) => {
  let parsedEvent;
  try {
    parsedEvent = typeof event.body === "string" ? JSON.parse(event.body) : event;
  } catch (parseError) {
    console.error("Failed to parse event body:", parseError);
    return {
      statusCode: 400,
      body: JSON.stringify({message: "Invalid event body format"}),
    };
  }

  const region = parsedEvent.region || "불명";
  const status = parsedEvent.status || false;
  const message = `${region}에서 재난이 발생했습니다.`;
  let phoneNumbers;

  if (region === "미지정" || !region) {
    console.warn("Region is missing or undefined for SNS notification.");
    return {
      statusCode: 400,
      body: JSON.stringify({message: "유효한 지역 정보가 필요합니다."}),
    };
  }

  try {
    phoneNumbers = await getPhoneNumbersByRegionFromDynamoDB(region);
    if (!phoneNumbers.length) {
      console.log(`💡 ${region} 지역에 등록된 전화번호가 없어 SMS를 보내지 않습니다.`);
      return {
        statusCode: 200,
        body: JSON.stringify({message: `SMS 알림 전송 대상 없음 (${region} 지역)`, region: region}),
      };
    }
  } catch (error) {
    console.error("전화번호 조회 실패:", error.message);
    return {
      statusCode: 500,
      body: JSON.stringify({message: "전화번호 조회 실패", error: error.message}),
    };
  }

  const sendSms = async (number) => {
    const formattedNumber = formatPhoneNumberForSns(number);

    if (!formattedNumber) {
      console.error(`❌ 유효하지 않은 전화번호 형식입니다: ${number}`);
      return {number, error: "Invalid phone number format"};
    }
    try {
      await sns.publish({
        Message: message,
        PhoneNumber: formattedNumber, // <-- 이 부분을 수정했습니다.
        MessageAttributes: {
          "AWS.SNS.SMS.SMSType": {
            DataType: "String",
            StringValue: "Transactional",
          },
        },
      });
      console.log(`${formattedNumber} 전송 성공`);
      return null;
    } catch (error) {
      console.error(`❌ ${number} 전송 실패`, error.message);
      return {number, error: error.message};
    }
  };

  const results = await Promise.all(phoneNumbers.map(sendSms));
  const failed = results.filter((r) => r !== null);

  if (failed.length > 0) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "일부 또는 전체 SMS 전송 실패",
        failed,
        region: region,
        totalSent: phoneNumbers.length - failed.length,
        totalFailed: failed.length,
      }),
    };
  }

  return {
    statusCode: 200,
    body: JSON.stringify({message: `SMS 전송 완료 (${region} 지역 ${phoneNumbers.length}명)`, region: region}),
  };
};
