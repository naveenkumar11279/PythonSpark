
filepath = "D:\data\day1db.txt"
try:
    file = open(filepath)
except Exception as e:
    print(e)
else:
    contents = file.read()
    print(contents)
finally:
    print(f"Closing the file :{file.name}")