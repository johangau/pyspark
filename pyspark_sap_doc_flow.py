from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from datetime import date

START_DATE: date = date(2021, 1, 1)

@transform_df(
    Output("/data/contractpreceedingdocuments"),
    vbfa=Input("/data/sap/vbfa"),
)
def compute(vbfa):

    contract = vbfa.filter(
            (F.col("erdat") >= START_DATE) &
            (F.col("vbtyp_n").eqNullSafe("G")),
        ).select(
            F.col("vbelv").alias("contract_id"),
            F.col("posnv").alias("contract_item"),
            F.col("vbeln").alias("commitment_contract_id"),
            F.col("posnn").alias("commitment_contract_item"),
        )

    return contract
