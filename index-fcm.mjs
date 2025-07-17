import admin from "firebase-admin";
import dotenv from "dotenv";

dotenv.config();

const serviceAccount = JSON.parse(Buffer.from(process.env.FIREBASE_CREDENTIALS_BASE64, "base64").toString("utf-8"));

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

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
  const status =
    typeof parsedEvent.status === "boolean"
      ? parsedEvent.status
      : typeof parsedEvent.status === "string"
      ? parsedEvent.status.toLowerCase() === "true"
      : false;

  let fcmResultStatus;

  // status가 true일 때만 FCM 알림 발송
  if (status) {
    const payload = {
      topic: "all",
      notification: {
        title: "재난 발생",
        body: `${region}에서 재난이 발생했습니다.`,
      },
      webpush: {
        notification: {
          title: "재난 발생",
          body: `${region}에서 재난이 발생했습니다.`,
          // requireInteraction: true, // 필요시 주석 해제
        },
      },
      data: {
        link: "https://naver.com", // 실제 URL로 변경 필요
      },
    };

    try {
      await admin.messaging().send(payload);
      console.log("✅ FCM 전송 완료");
      fcmResultStatus = "성공";
    } catch (error) {
      console.error("❌ 알림 전송 실패:", error.message);
      fcmResultStatus = "실패";
    }
  } else {
    console.log("FCM 전송 조건(status가 true)이 충족되지 않아 알림을 보내지 않습니다.");
    fcmResultStatus = "조건_미충족_알림_미발송";
  }

  const statusCode = fcmResultStatus === "성공" || fcmResultStatus === "조건_미충족_알림_미발송" ? 200 : 500;
  const message =
    fcmResultStatus === "성공"
      ? "FCM 알림 전송 성공"
      : fcmResultStatus === "조건_미충족_알림_미발송"
      ? "FCM 전송 조건 미충족 (알림 미발송)"
      : "FCM 알림 전송 실패";

  return {
    statusCode: statusCode,
    body: JSON.stringify({
      message: message,
      fcmStatus: fcmResultStatus,
      region: region,
      status: status,
      timestamp: Math.floor(new Date().getTime() / 1000),
    }),
  };
};
