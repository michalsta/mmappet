from mmappet import open_dataset_dct

test = open_dataset_dct("test_indexed.mmappet")
idx = open_dataset_dct("test_indexed.mmappet/index.mmappet")

print("data:\n", test)
print("index:\n", idx)
