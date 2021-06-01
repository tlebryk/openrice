import os
import pandas as pd
path = r"data/blue_eat/"
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
    "facebook": "Facebook",
    "instagram": "Instagram",
    "openrice": "OpenRice",
}

# Parse these strs multiple lines until the next list str is found. 
# Generally order should be: 
# 原因:
# 【老闆/店員私下言論 / Comments in private occasions】
# ... 
# source:
#  ...
# 相關圖片:
# ...


LIST_STRS = {
    "原因" : "Reason",
    "source" : "source_links" ,
    "相關圖片" : "Related_links"
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

SOURCES = {
    "【官方資訊 / Official Information】" : "Official",
    "【傳媒報導 / Reported by media】" : "Media",
    "【老闆/店員私下言論 / Comments in private occasions】": "Comments",
    "【只屬傳言 / Hearsay ONLY】": "Hearsay"
}
""

DAYLEN = len("星期三")
data_ls = []
for file in os.listdir(path)[:1]:
    with open (f"{path}//{file}", "r", encoding="utf-8") as f:
        buffer = f.read()
        # buf2 = buffer.replace("\n\n", "\n")
        # first line is "shop information" which we drop
        split_by_line = buffer.split("\n")
        data = {}     
        for i, line in enumerate(split_by_line):
            index = line.find(":")
            if index: 
                sub_str = line[:index].strip().lower()
                if (category := CATEGORIES.get(sub_str)):
                    data[category] = line[index+1:].strip()
                elif (category := LIST_STRS.get(sub_str)):
                    j = i + 1
                    while j < len(split_by_line):
                        if split_by_line[j].strip(":") in LIST_STRS:
                            break
                        data[f"{category}{j-i+1}"] = split_by_line[j]
                        j+=1
                elif (category := DAYS.get(line[:DAYLEN])):
                    data[category] = line[DAYLEN+1:].strip()
        data_ls.append(data)
df = pd.DataFrame(data_ls)
# path = r"data/csvs/yellow_eat/"
df.to_csv(f"{path}yellow_eat.csv")







