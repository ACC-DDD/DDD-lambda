import dotenv from "dotenv";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {UpdateCommand, DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";

dotenv.config();

const ddbClient = new DynamoDBClient({region: process.env.AWS_REGION});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

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

  const id = parsedEvent.id || "불명";

  if (id === "불명") {
    console.warn("Update request received with undefined or '불명' region.");
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: "업데이트할 지역이 정해져있지 않습니다. 유효한 'region' 값이 필요합니다.",
      }),
    };
  }

  const status =
    typeof parsedEvent.status === "boolean"
      ? parsedEvent.status
      : typeof parsedEvent.status === "string"
      ? parsedEvent.status.toLowerCase() === "true"
      : false;

  const timestamp = Math.floor(new Date().getTime() / 1000);
  const tableName = process.env.DYNAMODB_TABLE_NAME_CCTV || "Cctv";

  const params = {
    TableName: tableName,
    Key: {
      id: id,
    },
    UpdateExpression: "SET #st = :newSt",
    ExpressionAttributeNames: {
      "#st": "status",
    },
    ExpressionAttributeValues: {
      ":newSt": status,
    },
    ReturnValues: "ALL_NEW",
  };

  let dbResultStatus;
  try {
    await ddbDocClient.send(new UpdateCommand(params));
    console.log(`✅ DynamoDB 'cctv ${id}' 아이템의 status: ${status}`);
    dbResultStatus = "성공";
  } catch (error) {
    console.error(`❌ DynamoDB 'cctv ${id}' 아이템의 status:`, error.message);
    dbResultStatus = "실패";
  }
  const statusCode = dbResultStatus === "성공" ? 200 : 500;
  const message = dbResultStatus === "성공" ? "DynamoDB 저장 성공" : "DynamoDB 저장 실패";

  return {
    statusCode: statusCode,
    body: JSON.stringify({
      message: message,
      dbStatus: dbResultStatus,
      id: id,
      status: status,
      timestamp: timestamp,
    }),
  };
};
