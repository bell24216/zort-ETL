import os
import json
import logging
import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import functions_framework

# -------------------------------------------------------------------------
# ดึงคำสั่ง API แยกเป็นฟังก์ชันย่อย
# -------------------------------------------------------------------------
def get_order(storename, apikey, apisecret, date, limit=1000):
    url = "https://open-api.zortout.com/v4/Order/GetOrders"
    headers = {
        'storename': storename,
        'apikey': apikey,
        'apisecret': apisecret,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    all_orders = []
    page = 1

    while True:
        params = {
            'limit': limit,
            'status': '1',
            'paymentstatus': '1',
            'paymentafter': date,
            'paymentbefore': date,
            'page': page
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            if data.get("list"):
                all_orders.extend(data["list"])
                page += 1
            else:
                logging.info("No more data or end of list.")
                break
        except Exception as e:
            logging.error(f"Error while fetching page {page}: {e}")
            break

    return all_orders

# -------------------------------------------------------------------------
# Main Cloud Function Entry
# -------------------------------------------------------------------------
@functions_framework.cloud_event
def main(request):
    try:
        # เวลาปัจจุบันไทย
        utc_now = datetime.utcnow()
        thai_time = utc_now + timedelta(hours=7)
        # ลบออก 1 วัน
        thai_time = thai_time - timedelta(days=1)

        date = thai_time.strftime("%Y-%m-%d")
        logging.info(f"Thai date now: {date}")

        storename = os.environ['STORENAME']
        apikey = os.environ['APIKEY']
        apisecret = os.environ['APISECRET']
        table_id = os.environ['TABLE_ID']
        dataset_id = os.environ['DATASET_ID']
        project_id = os.environ['PROJECT_ID']
        project = os.environ['PROJECT']
        
        
        logging.info(f"Loaded env for store: {storename}")

        # ดึงข้อมูล order ทั้งหมด
        orders = get_order(storename, apikey, apisecret, date)
        logging.info(f"Fetched {len(orders)} orders")

        # ✅ ถ้าไม่มีออเดอร์เลย
        if not orders or len(orders) == 0:
            logging.info(f"📭 ไม่มีข้อมูลออเดอร์ในวันที่ {date}")
            print(f"📭 ไม่มีข้อมูลออเดอร์ในวันที่ {date}")
            return

        # แปลง orders เป็น DataFrame
        flattened = [
            {
                "orderId": order["number"],
                "id": order["id"],
                "customername": order["customername"],
                "orderdateString": order["orderdateString"],
                "updatedatetimeString": datetime.strptime(order['updatedatetimeString'], "%Y-%m-%d %H:%M").strftime("%Y-%m-%d"),
                "paymentdatetimeString": (
                    datetime.strptime(order['payments'][0]['paymentdatetimeString'], "%Y-%m-%d %H:%M").strftime("%Y-%m-%d")
                    if order.get('payments') and len(order['payments']) > 0
                    else None
                ),
                "receivedateString": order["receivedateString"],
                "successDateString": order["successDateString"],
                "order_status": order["status"],
                "paymentstatus": order["paymentstatus"],
                "saleschannel": order["saleschannel"],
                "productid": item["productid"],
                "sku": item["sku"],
                "name": item["name"],
                "quantity": item["number"],
                "unittext": item["unittext"],
                "totalprice_pretax": item["totalprice_pretax"],
                "totalprice_vat": item["totalprice_vat"],
                "totalprice": item["totalprice"],
                "discountamount": order["discountamount"] if item is order["list"][0] else 0,
                "voucheramount": order["voucheramount"] if item is order["list"][0] else 0,
                "shippingVoucher": order["shippingVoucher"] if item is order["list"][0] else 0,
                "totalproductamount": order["totalproductamount"] if item is order["list"][0] else 0,
                "buyerAmount": order["buyerAmount"] if item is order["list"][0] else 0,
                "trackingno": order["trackingno"],
                "shippingchannel": order["shippingchannel"],
                "customeremail": order.get("customeremail"),
                "customerphone": order.get("customerphone"),
                "customeraddress": order.get("customeraddress"),
                "warehousecode": order.get("warehousecode"),
            }
            for order in orders
            for item in order.get("list", [])
        ]

        df = pd.DataFrame(flattened)
        df = df[~df['saleschannel'].isin(['Lazada', 'Shopee', 'TIKTOK'])]
        
        # ✅ ถ้าไม่มีข้อมูลหลัง filter
        if df.empty:
            logging.info(f"📭 ไม่มีข้อมูลที่ต้องบันทึกในวันที่ {date}")
            print(f"📭 ไม่มีข้อมูลที่ต้องบันทึกในวันที่ {date}")
            return

        # logging.info(f"DataFrame shape: {df.shape}")
        # print(df.head())

        client_bigquery = bigquery.Client(project=project)  # เพิ่ม project ID

        query = f"""
        delete {project}.{dataset_id}.{table_id}
        where PARSE_DATE('%Y-%m-%d', successDateString)  = '{date}'
        """

        query_job = client_bigquery.query(query)

        # รอให้คำสั่งทำงานเสร็จ
        query_job.result()

        print(f"Data deleted {date} successfully.")

        df["id"] = df["id"].astype(str)
        df["productid"] = df["productid"].astype(str)
        # โหลดเข้า BigQuery
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"✅ Inserted {len(df)} rows to {table_ref}")

    except Exception as e:
        logging.error(f"❌ Error processing event: {e}")
        raise e
