from db import connect_db, init_table, insert_data
import asyncio

data_mockup = {
    # L0-L7 (TEXT)
    "L0": "ON",
    "L1": "OFF",
    "L2": "ON",
    "L3": "OFF",
    "L4": "ON",
    "L5": "OFF",
    "L6": "ON",
    "L7": "OFF",

    "Coutinue": "True",
    "Master_on": "True",
    "Machine_fault": "False",

    # LH (INTEGER)
    "LH_ASN": 1001,
    "LH_BN": 25,
    "LH_FillingPressure": 350,
    "LH_FillingVolume": 1200,
    "LH_PGNum": 3,
    "LH_StepNum": 5,
    "LH_VacuumGun": 1,
    "LH_VacuumPump": 1,
    "LH_PumpVacuumCurrent": 2.45,   # REAL

    # RH (INTEGER)
    "RH_ASN": 1002,
    "RH_BN": 26,
    "RH_FillingPressure": 348,
    "RH_FillingVolume": 1180,
    "RH_PGNum": 3,
    "RH_StepNum": 4,
    "RH_VacuumGun": 0,
    "RH_VacuumPump": 1,
    "RH_PumpVacuumCurrent": 2.38,   # REAL

    # Shared pumps (REAL)
    "PumpDegassingCurrent": 1.75,
    "PumpSupplyCurrent": 3.10,
    "PumpFillingCurrent": 2.90,
}

async def main():
    pool = await connect_db()
    print("connected")
    if pool is None:
        return
    await init_table(pool)
    await insert_data(pool, data_mockup)





asyncio.run(main(),loop_factory=asyncio.SelectorEventLoop)