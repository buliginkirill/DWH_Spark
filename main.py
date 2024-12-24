#!./.venv/bin/python
from src import app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
import os

# from src import DataLoader
# dl = DataLoader()
# dl.load_source_ddl()
# dl.load_marts_ddl()
# dl.load_data()

# from src import Processor
#
# p = Processor()
# p.erase_all_data()
# p.load_all_data()
#
