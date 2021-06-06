import os
import pandas as pd
folder = "blue_eat"
path = f"data/raw/{folder}/"
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
    # "原因" : "Reason",
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

# types of reasons under "原因"
SOURCES = {
    "官方資訊 " : "Official",
    "傳媒報導" : "Media",
    "老闆/店員私下言論 ": "Comments",
    "只屬傳言": "Hearsay"
}



DAYLEN = len("星期三")
data_ls = []
for file in os.listdir(path):
    if "csvs" in file: 
        continue
    with open (fr"{path}/{file}", "r", encoding="utf-8") as f:
        buffer = f.read()
        # buf2 = buffer.replace("\n\n", "\n")
        # first line is "shop information" which we drop
    split_by_line = buffer.split("\n")
    data = {"filename": file}     
    for i, line in enumerate(split_by_line):
        index = line.find(":")
        if index: 
            sub_str = line[:index].strip().lower()
            if (category := CATEGORIES.get(sub_str)):
                data[category] = line[index+1:].strip()
            # elif (sub_str == "原因"):
                source_type = "No_source"

        cate = "Reason"
        for source in SOURCES:
            if source in split_by_line[i]:
                k = 1
                source_type = SOURCES[source]
                j = i+1
                while j < len(split_by_line):
                    if split_by_line[j].strip(":").lower() in LIST_STRS or \
                    split_by_line[j].strip(":").lower() in CATEGORIES:
                        break
                    data[f"{cate}_{source_type}_{k}"] = split_by_line[j]
                    k+=1
                    j+=1
            
            elif (category := LIST_STRS.get(sub_str)):
                j = i + 1
                k = 1
                while j < len(split_by_line):
                    if split_by_line[j].strip(":") in LIST_STRS:
                        break                    
                    data[f"{category}_{k}"] = split_by_line[j]
                    k+=1
                    j+=1
            elif (category := DAYS.get(line[:DAYLEN])):
                data[category] = line[DAYLEN+1:].strip()
    data_ls.append(data)
df = pd.DataFrame(data_ls)
# path = r"data/csvs/yellow_eat/"
df.to_csv(f"{path}csvs/{folder}.csv", encoding="utf-8", index=False)







