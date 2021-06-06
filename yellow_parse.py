import os
import pandas as pd
folder = "yellow_shop"
path = f"data/{folder}/"
if not os.path.exists(f'{path}csvs/'):
    os.makedirs(f'{path}csvs/')
# match cateogories to strings: 
CATEGORIES = {   
    "店名": "Name",
    "類別": "Type",
    "地址": "Address",
    "電話": "Number",
    # 營業時間blank line; ignore
    "營業時間" : None,
    "網址": "Website",
    "網址/電郵:" : "Website",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "openrice": "OpenRice",
}
DAYS = {
    "星期一": "Monday",
    "星期二": "Tuesday",
    "星期三": "Wednesday",
    "星期四": "Thursday",
    "星期五": "Friday",
    "星期六": "Saturday",
    "星期日": "Sunday",
}
LIST_STRS = {
    "產品/服務": "Product-Service",
}
DAYLEN = len("星期三")
data_ls = []
for file in os.listdir(path):
    if "csvs" in file: 
        continue
    with open (f"{path}//{file}", "r", encoding="utf-8") as f:
        buffer = f.read()
        # first line is "shop information" which we drop
        split_by_line = buffer.split("\n")
        data = {'filename' : file}
        

        for i, line in enumerate(split_by_line):
            index = line.find(":")
            if index: 
                sub_str = line[:index].strip().lower()
                if (category := CATEGORIES.get(sub_str)):
                    data[category] = line[index+1:].strip()
                if sub_str == "產品/服務":
                    prods = split_by_line[i+1]
                    prods = prods.split(",")
                    for i, prod in enumerate(prods):
                        data[f"Product_{i}"] = prod
            if (category := DAYS.get(line[:DAYLEN])):
                data[category] = line[DAYLEN+1:].strip()
            
    data_ls.append(data)
df = pd.DataFrame(data_ls)
# path = r"data/csvs/yellow_eat/"
df.to_csv(f"{path}csvs/{folder}.csv", encoding="utf-8", index=False)







