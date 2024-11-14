import sys

from destination_databricks import DestinationDatabricks


def run():
    DestinationDatabricks().run(sys.argv[1:])


if __name__ == "__main__":
    run()
