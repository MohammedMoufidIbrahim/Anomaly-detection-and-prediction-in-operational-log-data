import pandas as pd

SOURCE_PATH = '/usr/src/app/dataset/operational_log.csv'

dataset = pd.read_csv(SOURCE_PATH)

i = 0

def send_message() -> dict:
    global i
    message = dataset[i:i + 1].to_dict('r')
    i += 1
    return message

