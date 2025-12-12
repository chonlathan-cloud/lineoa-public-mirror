import io
import os
from google.cloud import vision

def detect_text(path):
    """Detects text in the file."""
    
    # 1. สร้าง Client สำหรับเรียกใช้ Vision API
    client = vision.ImageAnnotatorClient()

    # 2. อ่านไฟล์จาก Path ที่คุณเตรียมไว้
    # ตรวจสอบว่าไฟล์มีอยู่จริงไหมเพื่อป้องกัน Error
    if not os.path.exists(path):
        print(f"Error: ไม่พบไฟล์ที่ {path}")
        return

    with io.open(path, 'rb') as image_file:
        content = image_file.read()

    image = vision.Image(content=content)

    # 3. เรียกใช้ Feature TEXT_DETECTION (เหมาะสำหรับ text ทั่วไป)
    # หรือถ้าเป็นเอกสารที่มีความหนาแน่นสูง (Dense text) อาจใช้ document_text_detection
    response = client.text_detection(image=image)
    texts = response.text_annotations

    print('Texts:')
    print('=' * 30)

    # response.text_annotations[0] คือข้อความทั้งหมดที่อ่านได้รวมกัน
    if texts:
        print(f"\n{texts[0].description}")
    else:
        print("ไม่พบข้อความในรูปภาพ")

    # ส่วนนี้ใช้เช็ค Error จาก API
    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))

# --- Main Execution ---
if __name__ == "__main__":
    # Path รูปภาพของคุณ
    file_path = '/Users/chonlathansongsri/Documents/company/line OA/data/slip.JPG'
    
    detect_text(file_path)