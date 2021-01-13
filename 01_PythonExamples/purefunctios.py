def pure_func(List):
    New_List = []

    for i in List:
        New_List.append(i ** 2)

    return New_List


# Driver's code
Original_List = [1, 2, 3, 4]
Modified_List = pure_func(Original_List)

print("Original List:", Original_List)
print("Modified List:", Modified_List)