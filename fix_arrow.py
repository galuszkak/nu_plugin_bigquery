with open("src/arrow_ipc.rs", "r") as f:
    content = f.read()

content = "use arrow::array::*;\n" + content

with open("src/arrow_ipc.rs", "w") as f:
    f.write(content)
