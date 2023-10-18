import asyncio
import random
import time

from pyflink.common import Row
from pyflink.table import DataTypes, AggregateFunction
from pyflink.table.udf import udf, udtf, udaf, udtaf, ACC


@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())
def mask_fn(uuid: str):
    return "xxxx-xxxx-xxxx-" + uuid.split("-")[-1][:4]


@udtf(input_types=DataTypes.STRING(), result_types=[DataTypes.STRING(), DataTypes.INT()])
def split_fn(s: str):
    for c in s.split(" "):
        yield c, len(c)


class GroupCount(AggregateFunction):
    def create_accumulator(self):
        return Row(0)

    def get_value(self, accumulator: ACC):
        return accumulator[0]

    def accumulate(self, accumulator: ACC, value: str):
        accumulator[0] += 1

    def retract(self, accumulator, value: str):
        accumulator[0] -= 1


group_count = udaf(
    GroupCount(),
    input_types=DataTypes.STRING(),
    result_type=DataTypes.BIGINT(),
    accumulator_type=DataTypes.ROW([DataTypes.FIELD("f0", DataTypes.BIGINT())]),
)


## external service lookup
@udf(
    input_types=DataTypes.STRING(),
    result_type=DataTypes.ROW(
        [
            DataTypes.FIELD("resp_1", DataTypes.STRING()),
            DataTypes.FIELD("resp_2", DataTypes.STRING()),
            DataTypes.FIELD("resp_3", DataTypes.STRING()),
            DataTypes.FIELD("total_resp_time", DataTypes.FLOAT()),
        ]
    ),
)
def async_lookup(transaction_id: str):
    async def make_request(transaction_id: str, req_num: int):
        delay = int(random.random() * 5)
        await asyncio.sleep(delay)
        return f"{transaction_id} req_num {req_num} took for {delay} seconds"

    async def gather(transaction_id: str):
        s = time.perf_counter()
        resp = await asyncio.gather(
            make_request(transaction_id, 1),
            make_request(transaction_id, 2),
            make_request(transaction_id, 3),
        )
        return Row(resp[0], resp[1], resp[2], round(time.perf_counter() - s, 4))

    return asyncio.run(gather(transaction_id))


# def _async_lookup(transaction_id: str):
#     async def make_request(transaction_id: str, req_num: int):
#         delay = int(random.random() * 5)
#         await asyncio.sleep(delay)
#         return f"{transaction_id} req_num {req_num} took for {delay} seconds"

#     async def gather(transaction_id: int):
#         s = time.perf_counter()
#         resp = await asyncio.gather(
#             make_request(transaction_id, 1),
#             make_request(transaction_id, 1),
#             make_request(transaction_id, 1),
#         )
#         return Row(resp[0], resp[1], resp[2], round(time.perf_counter() - s, 4))

#     return asyncio.run(gather(transaction_id))


# if __name__ == "__main__":
#     print(_async_lookup("001"))
