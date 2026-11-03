import asyncpg

async def connect_db():
    try:
        return await asyncpg.create_pool(
        host="localhost",
        port=5432,
        user="postgres",              # POSTGRES_USER
        password="1234@abcd",  # POSTGRES_PASSWORD
        database="postgres",     # POSTGRES_DB
)
    except Exception as e:
        print(f"connect_db error: {e}")
        return None


async def init_table(pool):
    try:
        async with pool.acquire() as conn:
            # ---- ตารางข้อมูลดิบรายวินาที ----
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS Machine (
                    Id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

                    -- L0-L7 (station/line values)
                    L0                      TEXT,
                    L1                      TEXT,
                    L2                      TEXT,
                    L3                      TEXT,
                    L4                      TEXT,
                    L5                      TEXT,
                    L6                      TEXT,

                    Continue                BOOLEAN,
                    Master_on               BOOLEAN,
                    Machine_fault           BOOLEAN,

                    -- LH
                    LH_ASN                  INTEGER,
                    LH_BN                   INTEGER,
                    LH_FillingPressure      INTEGER,
                    LH_FillingVolume        INTEGER,
                    LH_PGNum                INTEGER,
                    LH_StepNum              INTEGER,
                    LH_VacuumGun            INTEGER,
                    LH_VacuumPump           INTEGER,
                    LH_PumpVacuumCurrent    REAL,

                    -- RH
                    RH_ASN                  INTEGER,
                    RH_BN                   INTEGER,
                    RH_FillingPressure      INTEGER,
                    RH_FillingVolume        INTEGER,
                    RH_PGNum                INTEGER,
                    RH_StepNum              INTEGER,
                    RH_VacuumGun            INTEGER,
                    RH_VacuumPump           INTEGER,
                    RH_PumpVacuumCurrent    REAL,

                    -- Shared pumps
                    PumpDegassingCurrent    REAL,
                    PumpSupplyCurrent       REAL,
                    PumpFillingCurrent      REAL,

                    -- Timestamp
                    Recorded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """)

            # ---- ตารางเก็บผล predict (features / input / recon เป็น JSONB) ----
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS prediction_break_oil_filling_machine(
                    Id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

                    side            TEXT        NOT NULL,          -- 'LH' / 'RH'
                    pgnum           INTEGER     NOT NULL,
                    bn              INTEGER,                        -- วนซ้ำได้ -> ไม่ใช้เป็น key
                    asn             INTEGER,
                    predicted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

                    length          INTEGER,                        -- จำนวนวินาทีของคัน
                    overall_pass    BOOLEAN,

                    features        JSONB,                          -- สรุปต่อ feature ทั้งก้อน
                    input           JSONB,                          -- time-series ค่าจริง
                    recon           JSONB                           -- time-series ที่โมเดลสร้างคืน
                )
            """)
    except Exception as e:
        print(f"init_table error: {e}")


async def insert_data(pool, data, table="Machine"):
    try:
        columns = list(data.keys())
        values = list(data.values())

        col_str = ", ".join(columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(values)))


        query = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"

        async with pool.acquire() as conn:
            await conn.execute(query, *values)
    except Exception as e:
        print(f"insert_data error: {e}")
