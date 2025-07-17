import admin from "firebase-admin";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, PutCommand, DeleteCommand} from "@aws-sdk/lib-dynamodb";
import {ConditionalCheckFailedException} from "@aws-sdk/client-dynamodb";

const TABLE_NAME = "FcmToken";
const TOKEN_EXPIRATION_SECONDS = 60 * 60 * 24 * 90;

// Firebase Admin SDK 초기화
if (!admin.apps.length) {
  try {
    const serviceAccount = JSON.parse(Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, "base64").toString("utf-8"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  } catch (e) {
    console.error("[CRITICAL_ERROR] Firebase Admin SDK 초기화 실패:", e.message);
  }
}
const messaging = admin.messaging();
const ddbClient = new DynamoDBClient({region: process.env.AWS_REGION || "ap-northeast-2"});
const dynamoDB = DynamoDBDocumentClient.from(ddbClient);

// Lambda 핸들러 함수
export const handler = async (event) => {
  const successfulMessageIds = [];
  const failedMessageIds = [];

  for (const record of event.Records) {
    let token, topic;
    const messageId = record.messageId; // 각 레코드 처리의 성공 여부를 추적

    let recordProcessedSuccessfully = false;

    try {
      const parsedBody = JSON.parse(record.body);
      token = parsedBody.token;
      topic = parsedBody.topic;

      if (!token || typeof token !== "string" || !topic || typeof topic !== "string") {
        console.error(
          `[ERROR_MISSING_PARAMS] SQS Message ID: ${messageId} - token 또는 topic이 누락되었거나 형식이 올바르지 않습니다.`,
          {token, topic}
        );
        failedMessageIds.push({itemIdentifier: messageId});
        continue;
      }

      const now = new Date();
      const timestamp = Math.floor(now.getTime() / 1000);
      const expireAt = timestamp + TOKEN_EXPIRATION_SECONDS; 

      // 1. FCM 더미 메시지 전송 (토큰 유효성 검사 목적)
      try {
        await messaging.send({
          token,
          data: {type: "validate_fcm_token"},
        });
        console.log(`[INFO] SQS Message ID: ${messageId} - FCM 토큰 유효성 검사 성공: ${token.substring(0, 10)}...`);
      } catch (error) {
        const errorCode = error.errorInfo?.code;
        console.error(
          `[ERROR_FCM_VALIDATE] SQS Message ID: ${messageId} - FCM 전송 실패 (토큰 검증): ${
            errorCode || "UNKNOWN_FCM_ERROR"
          } - ${error.message}`,
          {token: token.substring(0, 10), topic, rawError: error}
        );

        if (errorCode === "messaging/registration-token-not-registered" || errorCode === "messaging/invalid-argument") {
          try {
            await dynamoDB.send(new DeleteCommand({TableName: TABLE_NAME, Key: {token}}));
            console.log(
              `[INFO] SQS Message ID: ${messageId} - 유효하지 않은 토큰 DynamoDB에서 삭제 완료: ${token.substring(
                0,
                10
              )}...`
            );
          } catch (dbDeleteError) {
            console.error(
              `[ERROR_DB_DELETE] SQS Message ID: ${messageId} - 유효하지 않은 토큰 삭제 실패: ${dbDeleteError.message}`,
              {
                token: token.substring(0, 10),
                dbDeleteError,
              }
            );
          }
          failedMessageIds.push({itemIdentifier: messageId});
          continue;
        } else {
          console.error(`[ERROR_FCM_UNKNOWN] SQS Message ID: ${messageId} - 알 수 없는 FCM 전송 오류.`, {
            rawError: error,
          });
          failedMessageIds.push({itemIdentifier: messageId});
          continue;
        }
      }
      
      // 2. DynamoDB에 토큰 정보 저장 (조건부 쓰기: 토큰이 존재하지 않을 때만 저장)
      let isNewToken = false;
      try {
        await dynamoDB.send(
          new PutCommand({
            TableName: TABLE_NAME,
            Item: {
              token,
              createdAt: timestamp,
              expireAt,
              topics: [topic],
              lastSubscriptionAttemptAt: timestamp,
            },
            ConditionExpression: "attribute_not_exists(#tokenAlias)",
            ExpressionAttributeNames: {
              "#tokenAlias": "token",
            },
          })
        );
        isNewToken = true;
        console.log(`[INFO] SQS Message ID: ${messageId} - 새 토큰 DynamoDB에 저장 성공: ${token.substring(0, 10)}...`);
      } catch (error) {
        if (error instanceof ConditionalCheckFailedException) {
          console.log(
            `[INFO] SQS Message ID: ${messageId} - 토큰이 이미 DynamoDB에 존재함 (건너뜀): ${token.substring(0, 10)}...`
          );
          isNewToken = false;
        } else {
          console.error(`[ERROR_DB_SAVE] SQS Message ID: ${messageId} - DynamoDB 저장 실패: ${error.message}`, {
            token: token.substring(0, 10),
            topic,
            rawError: error,
          });
          failedMessageIds.push({itemIdentifier: messageId});
          continue;
        }
      }
      
      // 3. 새 토큰인 경우에만 FCM 토픽 구독 시도 및 DynamoDB 업데이트
      let fcmSubStatus = "N/A";
      if (isNewToken) {
        try {
          await messaging.subscribeToTopic([token], topic);
          fcmSubStatus = "SUBSCRIBED";
          console.log(
            `[INFO] SQS Message ID: ${messageId} - FCM 토픽 구독 성공: ${token.substring(0, 10)}... - ${topic}`
          );
          await dynamoDB.send(
            new PutCommand({
              TableName: TABLE_NAME,
              Item: {
                token,
                createdAt: timestamp,
                expireAt,
                topics: [topic],
                fcmSubStatus,
                lastSubscriptionAttemptAt: timestamp,
              },
            })
          );
        } catch (error) {
          console.error(`[ERROR_FCM_SUBSCRIBE] SQS Message ID: ${messageId} - FCM 토픽 구독 실패: ${error.message}`, {
            token: token.substring(0, 10),
            topic,
            rawError: error,
          });
          // 구독 실패
          fcmSubStatus = "FAILED";
          await dynamoDB.send(
            new PutCommand({
              TableName: TABLE_NAME,
              Item: {
                token,
                createdAt: timestamp,
                expireAt,
                topics: [topic],
                fcmSubStatus,
                lastSubscriptionAttemptAt: timestamp,
              },
            })
          );
        }
      } else {
        // 이미 존재하는 토큰이며 유효성 검사를 통과한 경우
        // expireAt 최신 정보로 업데이트 고려
      }

      
      recordProcessedSuccessfully = true;
    } catch (overallRecordError) {
      console.error(
        `[CRITICAL_ERROR_RECORD] SQS Message ID: ${messageId} - 레코드 처리 중 예상치 못한 오류 발생:`,
        overallRecordError.message,
        {
          rawError: overallRecordError,
          messageBody: record.body,
        }
      );
    } finally {
      if (recordProcessedSuccessfully) {
        successfulMessageIds.push(messageId);
      } else {
        failedMessageIds.push({itemIdentifier: messageId});
      }
    }
  }

  if (failedMessageIds.length > 0) {
    return {
      statusCode: 200,
      batchItemFailures: failedMessageIds,
      body: JSON.stringify({
        message: `일부 메시지 처리 실패. ${successfulMessageIds.length}개 성공, ${failedMessageIds.length}개 실패.`,
        successfulMessageIds,
        failedMessageIds: failedMessageIds.map((item) => item.itemIdentifier),
      }),
    };
  } else {
    return {
      statusCode: 200,
      body: JSON.stringify("All SQS messages processed successfully!"),
    };
  }
};
