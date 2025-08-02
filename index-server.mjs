import https from "https";

export const handler = async (event) => {
  const TARGET_IP = process.env.TARGET_IP;
  const TARGET_PATH = process.env.TARGET_PATH;
  if (!TARGET_IP || !TARGET_PATH) {
    console.error("환경 변수 TARGET_IP 또는 TARGET_PATH가 설정되지 않았습니다.");
    return {
      statusCode: 500,
      body: JSON.stringify({message: "서버 설정 오류: 엔드포인트 정보 누락"}),
    };
  }

  let parsedEvent;
  try {
    parsedEvent = typeof event.body === "string" ? JSON.parse(event.body) : event;
  } catch (parseError) {
    console.error("이벤트 바디 파싱 실패:", parseError);
    return {
      statusCode: 400,
      body: JSON.stringify({message: "잘못된 이벤트 바디 형식입니다."}),
    };
  }

  const region = parsedEvent.region || "불명";
  if (region === "불명") {
    console.warn("업데이트 요청에 유효한 region 값이 없습니다.");
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

  const payload = JSON.stringify({region, status});
  const options = {
    hostname: TARGET_IP,
    port: 8080,
    path: TARGET_PATH,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(payload),
    },
  };

  // --- HTTPS POST 요청 로직 ---
  try {
    const serverResponse = await new Promise((resolve, reject) => {
      const req = https.request(options, (res) => {
        let responseBody = "";

        res.on("data", (d) => {
          responseBody += d;
        });

        res.on("end", () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            console.log(`요청 성공. 상태 코드: ${res.statusCode}`);
            console.log(`응답 본문: ${responseBody}`);
            resolve({
              statusCode: 200,
              body: JSON.stringify({message: "데이터를 서버에 성공적으로 전송했습니다."}),
            });
          } else {
            console.error(`요청 실패! 상태 코드: ${res.statusCode}`);
            resolve({
              statusCode: res.statusCode,
              body: JSON.stringify({message: `데이터 전송 실패. 서버 응답: ${responseBody}`}),
            });
          }
        });
      });

      req.on("error", (e) => {
        console.error(`네트워크 오류 발생: ${e.message}`);
        reject(e);
      });

      // 페이로드 전송
      req.write(payload);
      req.end();
    });

    return serverResponse;
  } catch (error) {
    console.error(`요청 처리 중 예외 발생: ${error.message}`);
    return {
      statusCode: 500,
      body: JSON.stringify({message: `요청 중 오류 발생: ${error.message}`}),
    };
  }
};
