/**
 * Configuration Section
 * ---------------------
 * กรุณากรอกข้อมูลของคุณที่นี่
 */
const CONFIG = {
  // รหัสโปรเจกต์ Google Cloud ของคุณ
  GCP_PROJECT_ID: "lineoa-g49",

  // อีเมลของ Service Account ที่คุณใช้งาน
  SERVICE_ACCOUNT_EMAIL: "YOUR_SERVICE_ACCOUNT_EMAIL@your-project-id.iam.gserviceaccount.com",

  // The private key from your service account JSON file.
  // IMPORTANT: Replace the newline characters (
) in the JSON file with \n as shown.
  SERVICE_ACCOUNT_PRIVATE_KEY: "-----BEGIN PRIVATE KEY-----\n...YOUR...PRIVATE...KEY...\n-----END PRIVATE KEY-----\n",
};


// =================================================================================================
// ไม่ต้องแก้ไขโค้ดด้านล่างนี้ หากไม่แน่ใจ
// =================================================================================================

/**
 * ฟังก์ชันหลักสำหรับจัดการขั้นตอนการลงทะเบียน
 */
function handleFormStep(event, session) {
  const userId = event.source.userId;
  const replyToken = event.replyToken;
  const messageType = event.message ? event.message.type : null;
  const now = new Date().getTime();

  // ขั้นตอนที่ 0: เริ่มต้น Workflow
  if (!session && messageType === "text" &&
    (event.message.text.trim().toLowerCase() === "เริ่มต้นเปิดร้านของคุณ" ||
      event.message.text.trim().toLowerCase() === "เริ่มต้นใหม่")) {
    const newSession = {
      step: 1,
      timestamp: now
    };
    cache.put(userId, JSON.stringify(newSession), 600); // 10 minute session
    const guideMessage = `ยินดีต้อนรับสู่ระบบลงทะเบียนร้านค้า! 😍\n
เรามี 4 ขั้นตอนง่าย ๆ:
1. ส่งชื่อ-นามสกุล(พิมพ์ 👇)
`;
    sendReply(replyToken, guideMessage);
    return;
  }

  if (!session) return;

  // ยกเลิก Workflow
  if (messageType === "text" && event.message.text.trim() === "ยกเลิก") {
    cache.remove(userId);
    sendReply(replyToken, "🙅‍♂️ ยกเลิกการกรอกข้อมูลแล้ว

หากต้องการเริ่มต้นใหม่ พิมพ์ \"เริ่มต้นเปิดร้านของคุณ\" หรือ \"เริ่มต้นใหม่\" ครับ");
    return;
  }

  // ขั้นตอนที่ 1: รับชื่อ
  if (session.step === 1 && messageType === "text") {
    session.name = event.message.text.trim();
    session.step = 2;
    session.timestamp = now;
    cache.put(userId, JSON.stringify(session), 600);
    sendReply(replyToken, createQuickReply("เยี่ยมเลย! ต่อไป 
2.เบอร์มือถือสำหรับติดต่อครับ
", ["ยกเลิก"]));
    return;
  }

  // ขั้นตอนที่ 2: รับเบอร์โทรศัพท์
  if (session.step === 2 && messageType === "text") {
    session.phone = event.message.text.trim();
    session.step = 3;
    session.timestamp = now;
    cache.put(userId, JSON.stringify(session), 600);
    sendReply(replyToken, createQuickReply("ยอดเยี่ยมครับ! ต่อไป 
3.ชื่อร้าน LineOA ที่คุณต้องการคืออะไรครับ?
", ["ยกเลิก"]));
    return;
  }

  // ขั้นตอนที่ 3: รับชื่อร้าน
  if (session.step === 3 && messageType === "text") {
    session.shop = event.message.text.trim();
    session.step = 4;
    session.timestamp = now;
    cache.put(userId, JSON.stringify(session), 600);
    sendReply(replyToken, createQuickReply("เกือบเสร็จแล้ว! ขั้นตอนสุดท้าย

**กรุณาแชร์ตำแหน่งร้านของคุณ**

ถ้าไม่มีหน้าร้าน เลือก “ร้านออนไลน์”", ["ยกเลิก", "ร้านออนไลน์"]));
    return;
  }

  // ขั้นตอนที่ 4: รับตำแหน่งร้าน
  if (session.step === 4 && (messageType === "location" || (messageType === "text" && event.message.text.trim().toLowerCase() === "ร้านออนไลน์"))) {
    if (messageType === "location") {
      session.location = {
        title: event.message.title || "ตำแหน่งร้าน",
        address: event.message.address,
        lat: event.message.latitude,
        lng: event.message.longitude
      };
    } else {
      session.location = null; // ร้านค้าออนไลน์
    }

    session.step = 5; // ไปยังขั้นตอนยืนยัน
    session.timestamp = now;
    cache.put(userId, JSON.stringify(session), 600);

    try {
      const profile = getUserProfile(userId);
      const flexMessage = buildSummaryFlex(session, profile);
      sendReply(replyToken, flexMessage);
    } catch (err) { // สมมติว่ามีฟังก์ชัน logErrorToSheet อยู่
      logErrorToSheet(err);
      sendReply(replyToken, "เกิดข้อผิดพลาดในการสร้างข้อมูลสรุป: " + err.message);
    }
    return;
  }

  // ขั้นตอนที่ 5: ยืนยันและบันทึกลง Firestore
  if (session.step === 5 && messageType === "text" && event.message.text.trim() === "ยืนยัน") {
    try {
      const accessToken = getGcpAccessToken();
      const shopId = getNextShopId(accessToken);
      const profile = getUserProfile(userId);

      saveShopToFirestore(accessToken, shopId, session, profile);

      cache.remove(userId);
      sendReply(replyToken, "✅ ลงทะเบียนร้านค้าของคุณสำเร็จแล้ว!
Shop ID: " + shopId + "

เราจะรีบดำเนินการตรวจสอบและติดต่อกลับไปครับ");

    } catch (err) {
      logErrorToSheet(err);
      sendReply(replyToken, "เกิดข้อผิดพลาดในการบันทึกข้อมูล: " + err.message);
    }
    return;
  }

  // ข้อความ fallback กรณีไม่เข้าเงื่อนไข
  sendReply(replyToken, "ขอโทษครับ ผมยังไม่เข้าใจ กรุณาส่งข้อมูลตามขั้นตอน หรือ พิมพ์ \"ยกเลิก\" เพื่อเริ่มใหม่");
}


/**
 * 2.ฟังค์ชั่นสร้าง Flex Message สรุปข้อมูลการลงทะเบียนทั้งหมด
 * (ไม่มีการเปลี่ยนแปลงในฟังก์ชันนี้)
 */
function buildSummaryFlex(session, profile) {
  const shop = session.shop || "(ไม่ระบุชื่อร้าน)";
  const name = session.name || "-";
  const phone = session.phone || "-";
  const address = session.location ? .address || "ธุรกิจออนไลน์ ไม่มีหน้าร้าน";
  const logoUrl = session.logoUrl || profile.pictureUrl || 
    `https://dummyimage.com/600x400/cccccc/000000&text=${encodeURIComponent(shop)}`;

  return {
    type: "flex",
    altText: "สรุปข้อมูลร้านค้า",
    contents: {
      type: "bubble",
      hero: {
        type: "image",
        url: logoUrl,
        size: "full",
        aspectRatio: "20:13",
        aspectMode: "cover"
      },
      body: {
        type: "box",
        layout: "vertical",
        contents: [{
          type: "text",
          text: shop,
          weight: "bold",
          size: "xl",
          color: "#1DB446",
          align: "start"
        }, {
          type: "text",
          text: "บัญชีร้านค้าอย่างเป็นทางการ",
          size: "sm",
          color: "#888888",
          align: "start",
          margin: "sm"
        }, {
          type: "separator",
          margin: "md"
        }, {
          type: "box",
          layout: "vertical",
          margin: "md",
          spacing: "sm",
          contents: [{
            type: "text",
            text: "ข้อมูลร้านค้า",
            weight: "bold",
            size: "md",
            color: "#000000",
            margin: "sm"
          }, {
            type: "box",
            layout: "vertical",
            spacing: "xs",
            contents: [{
              type: "box",
              layout: "baseline",
              spacing: "sm",
              contents: [{
                type: "text",
                text: "ชื่อผู้ติดต่อ",
                color: "#aaaaaa",
                size: "sm",
                flex: 2
              }, {
                type: "text",
                text: name,
                wrap: true,
                size: "sm",
                flex: 4
              }]
            }, {
              type: "box",
              layout: "baseline",
              spacing: "sm",
              contents: [{
                type: "text",
                text: "เบอร์โทร",
                color: "#aaaaaa",
                size: "sm",
                flex: 2
              }, {
                type: "text",
                text: phone,
                wrap: true,
                size: "sm",
                flex: 4
              }]
            }, {
              type: "box",
              layout: "baseline",
              spacing: "sm",
              contents: [{
                type: "text",
                text: "ที่อยู่ร้าน",
                color: "#aaaaaa",
                size: "sm",
                flex: 2
              }, {
                type: "text",
                text: address,
                wrap: true,
                size: "sm",
                flex: 4,
                color: "#444444"
              }]
            }]
          }]
        }, {
          type: "separator",
          margin: "xl"
        }, {
          type: "text",
          text: "💬: ส่งรูปโลโก้ร้าน, เมนูสินค้า, ช่องทางชำระเงินร้านของคุณในแชทนี้ หลังจากชำระเงินเสร็จแล้ว!",
          wrap: true,
          size: "xs",
          color: "#888888",
          margin: "lg",
          align: "center"
        }]
      },
      footer: {
        type: "box",
        layout: "vertical",
        contents: [{
          type: "button",
          style: "primary",
          color: "#1DB446",
          height: "sm",
          action: {
            type: "message",
            label: "ยืนยันข้อมูล",
            text: "ยืนยัน"
          }
        }],
        flex: 0
      }
    }
  };
}

/**
 * Sends a reply message to the user via the LINE Messaging API.
 * (ไม่มีการเปลี่ยนแปลงในฟังก์ชันนี้)
 */
function sendReply(replyToken, payload) {
  if (!replyToken) {
    Logger.log("sendReply: missing replyToken");
    return;
  }
  let messages;
  if (typeof payload === "string") {
    messages = [{
      type: "text",
      text: payload
    }];
  } else {
    messages = Array.isArray(payload) ? payload : [payload];
  }
  const url = "https://api.line.me/v2/bot/message/reply";
  const options = {
    method: "post",
    headers: {
      "Content-Type": "application/json",
      "Authorization": "Bearer " + lineToken(), // สมมติว่ามีฟังก์ชัน lineToken() อยู่
    },
    payload: JSON.stringify({
      replyToken: replyToken,
      messages: messages
    }),
    muteHttpExceptions: true
  };
  const resp = UrlFetchApp.fetch(url, options);
  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    Logger.log("sendReply error: HTTP " + code + " > " + resp.getContentText());
  }
}


// ===============================================================
// ฟังก์ชันเสริมใหม่สำหรับ GOOGLE CLOUD AUTHENTICATION & FIRESTORE
// ===============================================================

/**
 * Generates a Google Cloud Platform access token from a service account.
*/
function getGcpAccessToken() {
  const privateKey = CONFIG.SERVICE_ACCOUNT_PRIVATE_KEY;
  const serviceAccountEmail = CONFIG.SERVICE_ACCOUNT_EMAIL;
  const scope = "https://www.googleapis.com/auth/datastore";

  const jwtHeader = {
    alg: "RS256",
    typ: "JWT"
  };
  const now = Math.floor(Date.now() / 1000);
  const jwtClaimSet = {
    iss: serviceAccountEmail,
    scope: scope,
    aud: "https://oauth2.googleapis.com/token",
    exp: now + 3600, // Token valid for 1 hour
    iat: now,
  };

  const encodedJwtHeader = Utilities.base64EncodeWebSafe(JSON.stringify(jwtHeader));
  const encodedJwtClaimSet = Utilities.base64EncodeWebSafe(JSON.stringify(jwtClaimSet));
  const signatureInput = encodedJwtHeader + "." + encodedJwtClaimSet;
  const signature = Utilities.computeRsaSha256Signature(signatureInput, privateKey);
  const encodedSignature = Utilities.base64EncodeWebSafe(signature);
  const jwt = signatureInput + "." + encodedSignature;

  const tokenResponse = UrlFetchApp.fetch("https://oauth2.googleapis.com/token", {
    method: "post",
    contentType: "application/x-www-form-urlencoded",
    payload: {
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion: jwt,
    },
  });

  const accessToken = JSON.parse(tokenResponse.getContentText()).access_token;
  if (!accessToken) {
    throw new Error("Failed to obtain GCP access token.");
  }
  return accessToken;
}

/**
 * Generates the next sequential shop ID (e.g., shop_00001).
*/
function getNextShopId(accessToken) {
  const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.GCP_PROJECT_ID}/databases/(default)/documents:runQuery`;
  const payload = {
    structuredQuery: {
      from: [{
        collectionId: 'shops'
      }],
      select: {},
    }
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'Authorization': 'Bearer ' + accessToken
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, options);
  const content = resp.getContentText();
  const responseCode = resp.getResponseCode();

  if (responseCode >= 400) {
    throw new Error(`Firestore query failed with code ${responseCode}: ${content}`);
  }

  // Response คือ stream ของ object เราจะนับจำนวน object ที่ไม่ว่าง
  const documents = JSON.parse(content).filter(item => item.document);
  const count = documents.length;
  const nextNumber = count + 1;
  const paddedNumber = nextNumber.toString().padStart(5, '0'); // ทำให้เป็นเลข 5 หลัก เช่น 00001

  return `shop_${paddedNumber}`;
}

/**
 * บันทึกข้อมูลการลงทะเบียนไปยังเอกสารใน Firestore
 */
function saveShopToFirestore(accessToken, shopId, session, profile) {
  const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.GCP_PROJECT_ID}/databases/(default)/documents/shops/${shopId}/owner_profile/information`;

  const locationData = session.location ? {
    map: {
      title: { stringValue: session.location.title },
      address: { stringValue: session.location.address },
      geo: {
        geoPointValue: {
          latitude: session.location.lat,
          longitude: session.location.lng
        }
      }
    }
  } : {
    stringValue: "ออนไลน์"
  };

  const payload = {
    fields: {
      createdAt: { timestampValue: new Date().toISOString() },
      lineUserId: { stringValue: profile.userId },
      lineDisplayName: { stringValue: profile.displayName },
      contactName: { stringValue: session.name },
      phone: { stringValue: session.phone },
      shopName: { stringValue: session.shop },
      logoUrl: { stringValue: profile.pictureUrl },
      location: locationData
    }
  };

  const options = {
    method: 'patch', // ใช้ patch เพื่อสร้างหรือเขียนทับ
    contentType: 'application/json',
    headers: {
      'Authorization': 'Bearer ' + accessToken
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, options);
  const responseCode = resp.getResponseCode();
  if (responseCode < 200 || responseCode >= 300) {
    throw new Error(`Failed to save to Firestore. Status: ${responseCode} Body: ${resp.getContentText()}`);
  }
  Logger.log(`Successfully saved shop ${shopId} to Firestore.`);
}


/*
หมายเหตุ: ฟังก์ชันต่อไปนี้ถูกสมมติว่ามีอยู่แล้วในโปรเจกต์ของคุณตามโค้ดเดิม
- cache.put(), cache.get(), cache.remove() (likely from CacheService)
- createQuickReply()
- getUserProfile()
- logErrorToSheet()
- lineToken()
*/
