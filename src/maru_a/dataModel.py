class MachineReading:
    def __init__(self, tags, l0, l1, l2, l3, l4, l5, l6):
        self.tags = tags
        self.l = [l0, l1, l2, l3, l4, l5, l6]

    def to_postgres(self):
        data = dict(self.tags)
        data["L0"], data["L1"], data["L2"], data["L3"], data["L4"], data["L5"], data["L6"] = self.l
        return data

    def to_mqtt(self):
        return {
            "EVENT_TS": self.tags["recorded_at"],
            "COMPANY_CD": self.l[0],
            "PLANT_CD": self.l[1],
            "SHOP_CD": self.l[2],
            "LINE_ID": self.l[3],
            "STATION_CD": self.l[4],
            "FUNCTION_CD": self.l[5],
            "CE_CD": self.l[6],
            "ASSET_PATH": "/".join(self.l),
            "PAYLOAD": self.tags,
        }