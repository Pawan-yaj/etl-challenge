import logging

from database import Database

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)


class EtlScript:
    def __init__(self):
        self.database_conn = Database("acme")
        self.header_file = "headers.txt"
        self.data_file = "data.tsv"
        self.out_file = "output.csv"

    def load_file_to_database(self, file_path: str):
        self.database_conn.load_file(file_path)

    def run(self):
        try:
            with open(self.header_file, 'r') as file:
                schema = file.read().replace('\n', ',')
                logger.info(f"Schema {schema}")
                print(f"Schema {schema}")
        except FileNotFoundError:
            logger.error(f"The specified file {self.header_file} not found")
            raise

        try:
            with open(self.data_file, 'r+') as f:
                first_line = f.readline()
                content = f.read()
        except FileNotFoundError:
            logger.error(f"The specified file {self.data_file} not found")
            raise
            
        if schema in first_line:
            logger.info(f"Schema has been alredy existing in the file {self.data_file}")
            
        try:
            with open(self.out_file, 'w') as f:
                f.seek(0, 0)
                f.write(schema.rstrip('\r\n') + '\n' + content)
                logger.info("Schema has been added to the data file")
        except FileNotFoundError:
            logger.error(f"The specified file {self.out_file} not found")
            raise

        try:
            self.load_file_to_database(self.out_file)
        except BaseException:
            logger.error("Error loading data into the database")
            raise


if __name__ == "__main__":
    EtlScript().run()
