import pandas as pd


def clean_data(infile, outfile):
    chunk_size = 10 ** 4
    features = ["price",
                "city_fuel_economy", "highway_fuel_economy",
                "horsepower", "year", "mileage",
                "height", "length",
                "make_name", "model_name"]
    chunk_count = 0

    with pd.read_csv(infile, usecols=features, chunksize=chunk_size) as reader:
        for chunk in reader:
            # remove trailing " in" from height and length
            chunk["height"] = [str(val)[:-3] if str(val)[len(str(val)) - 3:] == " in" else val
                               for val in chunk["height"]]
            chunk["length"] = [str(val)[:-3] if str(val)[len(str(val)) - 3:] == " in" else val
                               for val in chunk["length"]]
            # write the chunk
            if chunk_count == 0:
                chunk.to_csv(outfile, index=False, mode="w")
            else:
                chunk.to_csv(outfile, index=False, mode="a")
            # progress update
            print("finished chunk {c}, about {s} rows total\n".format(
                c=chunk_count, s=(chunk_count + 1) * chunk_size))
            chunk_count += 1


def main():
    raw_filename = "/Users/andrewcheung/Downloads/used_cars_data.csv"
    clean_filename = "used_cars_data_CLEAN.csv"
    # we kept 10 out of the 66 features,
    # which shrank the file size from 10 GB to 176 MB
    clean_data(raw_filename, clean_filename)


if __name__ == '__main__':
    main()
