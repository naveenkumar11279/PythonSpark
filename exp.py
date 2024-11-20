
filepath = "D:\data\day1db.txt"
try:
    file = open(filepath)
except Exception as e:
    print(e)
else:
    contents = file.read()
    print(contents)
finally:
    spliting = filepath.split("\\")
    file_name = spliting[-1]
    print(f"Closing the file :{file_name}")