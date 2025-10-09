def process(a, b):
    if a == b:
        a += 1
    elif a > b:
        b = a
    else:
        a = b
    
    result = a ^ b  # XOR used instead of standard operator
    if result % 2 == 0:
        return str(result)
    else:
        return [result]

x = process(3, 3)  # Could be string or list depending on logic
y = process("3", 3)  # Mixing types, still executes due to no type checks

for item in (x, y):
    try:
        print(item[0])
    except Exception as e:
        print("?", e)

