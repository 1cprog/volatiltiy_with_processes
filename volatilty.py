# -*- coding: utf-8 -*-


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПРОЦЕССНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
from multiprocessing import Pool, cpu_count
from os import path, walk
from decimal import Decimal
from warnings import warn
from python_snippets.utils import time_track


def file_parser(filename):

    def file_reader():
        with open(filename, 'r', encoding='utf-8') as input_file:
            for string in input_file:
                yield string
    try:
        get_file_data = file_reader()
        next(get_file_data, None)

        secid, price_min, price_max = None, None, None
        for line in get_file_data:
            try:
                secid, trade_time, price, quantity = line.split(',')
            except ValueError as err:
                print(err, err.args)
                continue

            try:
                price = Decimal(price)

            except Exception as err:
                print(err, err.args)
                continue

            if price_max is None:
                price_min, price_max = Decimal(price), Decimal(price)

            price = Decimal(price)
            if price_max < price:
                price_max = price
            if price_min > price:
                price_min = price

        if price_min is None:
            warn(f'No data in file {filename}')
        else:
            half_sum = (price_min + price_max) / 2
            if half_sum:
                volatility = ((price_max - price_min) / half_sum) * 100
            else:
                volatility = 0
            return {'ticker_name': secid, 'volatility': volatility}
    except IOError as err:
        print(err, err.args)


class FileCrawler:

    def __init__(self, directory):
        self.directory = directory
        self.volatility_list = []
        self.zero_volatility_list = []
        
    def run(self):
        if not path.exists(self.directory):
            raise OSError('Directory not exist')

        for root, _, filelist in walk(self.directory):
            if __name__ == '__main__':

                with Pool(processes=cpu_count()) as pool:
                    result_list = pool.map(file_parser, [path.join(root, file) for file in filelist])
                    pool.close()
                    pool.join()

                for result in result_list:
                    if result['volatility']:
                        self.volatility_list.append(result)
                    else:
                        self.zero_volatility_list.append(result['ticker_name'])
                volatility_list = sorted(self.volatility_list, key=lambda val: val['volatility'], reverse=True)
                print('Максимальная волатильность:')
                for res in volatility_list[:3]:
                    print(f'\t{res["ticker_name"]} - {round(res["volatility"], 2)} %')
                print('Минимальная волатильность:')
                for res in volatility_list[-3:]:
                    print(f'\t{res["ticker_name"]} - {round(res["volatility"], 2)} %')
                print('Нулевая волатильность:')
                print(f'\t{self.zero_volatility_list}')


if __name__ == '__main__':
    operation = FileCrawler('./trades')
    operation.run()
