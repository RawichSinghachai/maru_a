"""
Demo: ทดสอบการเชื่อมต่อฐานข้อมูล PostgreSQL ด้วย asyncpg
ติดตั้งก่อนใช้งาน: pip install asyncpg
"""

import asyncio
import asyncpg


# แก้ไขค่าเชื่อมต่อให้ตรงกับฐานข้อมูลของคุณ
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "your_password",
    "database": "your_database",
}


async def test_connection():
    """ลองเชื่อมต่อฐานข้อมูล แล้วรายงานผลว่าต่อได้หรือไม่"""
    conn = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        version = await conn.fetchval("SELECT version();")
        print("เชื่อมต่อสำเร็จ ✅")
        print(f"PostgreSQL version: {version}")
        return True

    except asyncpg.InvalidPasswordError:
        print("เชื่อมต่อไม่สำเร็จ ❌ — username/password ไม่ถูกต้อง")
    except asyncpg.InvalidCatalogNameError:
        print("เชื่อมต่อไม่สำเร็จ ❌ — ไม่พบฐานข้อมูลชื่อนี้")
    except (ConnectionRefusedError, OSError) as e:
        print(f"เชื่อมต่อไม่สำเร็จ ❌ — ต่อ host/port ไม่ได้: {e}")
    except Exception as e:
        print(f"เชื่อมต่อไม่สำเร็จ ❌ — เกิดข้อผิดพลาด: {type(e).__name__}: {e}")
    finally:
        if conn is not None:
            await conn.close()

    return False


if __name__ == "__main__":
    asyncio.run(test_connection())