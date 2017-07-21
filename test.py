

"This module does blah blah."
def contains_even_number(my_list):
    "This module does blah blah."
    has_even_number = False
    for elt in my_list:
        if elt % 2 == 0:
            has_even_number = True
        break
    if has_even_number:
        print("list contains an even number")
    else:
        print("list does not contain an even number")


for n in range(2, 10):
    for x in range(2, n):
        if n % x == 0:
            print(n, 'equals', x, '*', n/x)
            break
    else:
        # loop fell through without finding a factor
        print(n, 'is a prime number')

print("True" if  float(0) == 0 else "False")
