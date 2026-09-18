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
                    L7                      TEXT,

                    Coutinue                TEXT,
                    Master_on               TEXT,
                    Machine_fault           TEXT,

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