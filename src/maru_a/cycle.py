"""
ประกอบข้อมูล OPC รายวินาทีให้เป็น "รถทีละคัน" — ทำที่ฝั่ง OPC (เครื่อง 1)
ตัด cycle ด้วย StepNum : ตัดตรงจุดที่ step เปลี่ยนจาก "ไม่ใช่ 0 -> 0"
  คันเก่าปิด (ไม่รวม 0 ตัวนี้), 0 ตัวนี้เป็นแถวแรกของคันใหม่ ; เก็บทุกแถวรวม 0 ที่ติดกัน
คัดคอลัมน์ต่อข้าง : LH -> LH_* + คอลัมน์กลาง / RH -> RH_* + คอลัมน์กลาง
"""


def select_side_columns(row: dict, side: str) -> dict:
    """คัด 1 แถว ให้เหลือเฉพาะคอลัมน์ของข้างนี้ + คอลัมน์กลาง (ตัดข้างตรงข้ามทิ้ง)"""
    other = "RH_" if side == "LH" else "LH_"
    return {k: v for k, v in row.items() if not k.upper().startswith(other)}


class CycleBuffer:
    """สะสมข้อมูลของรถ 1 คัน (ของข้างหนึ่ง) แล้วคายออกเมื่อจบคัน"""

    def __init__(self, side: str):
        self.side = side
        self.step_key = f"{side}_StepNum"
        self.rows = []
        self.prev_step = 0

    def update(self, tags: dict):
        """ป้อน tags 1 ชุด (ข้อมูล 1 วินาที จาก read_tags).
        คืน list ของแถว (คัดคอลัมน์ข้างนี้แล้ว, เรียงตาม datetime) เมื่อจบคัน, ไม่งั้นคืน None
        """
        step = tags.get(self.step_key)
        if step is None:
            return None

        completed = None
        # จุดตัด : "ไม่ใช่ 0 -> 0" = คันเก่าจบ
        if self.prev_step != 0 and step == 0:
            if self.rows:
                ordered = sorted(self.rows, key=lambda r: r["recorded_at"])
                completed = [select_side_columns(r, self.side) for r in ordered]
            self.rows = []        # ล้างก่อน -> 0 ตัวนี้จะไปเป็นแถวแรกของคันใหม่

        self.rows.append(dict(tags))  # copy เก็บทุกวินาที (รวม step=0)
        self.prev_step = step
        return completed