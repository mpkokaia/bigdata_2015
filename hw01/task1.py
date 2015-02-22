#!/usr/bin/python
# encoding: utf8

# Для быстрого локального тестирования используйте модуль test_dfs
import test_dfs as dfs

# Для настоящего тестирования используйте модуль http_dfs
#import http_dfs as dfs

# Демо показывает имеющиеся в DFS файлы, расположение их фрагментов
# и содержимое фрагмента "partitions" с сервера "cs0"
# (не рассчитывайте, что эти две константы останутся неизменными в http_dfs. Они
#  использованы исключительно для демонстрации)
def demo():
  for f in dfs.files():
    print("File {0} consists of fragments {1}".format(f.name, f.chunks))

  for c in dfs.chunk_locations():
    print("Chunk {0} sits on chunk server {1}".format(c.id, c.chunkserver))

  # Дальнейший код всего лишь тестирует получение фрагмента, предполагая, что известно,
  # где он лежит. Не рассчитывайте, что этот фрагмент всегда будет находиться
  # на использованных тут файл-серверах

  # При использовании test_dfs читаем из каталога cs0
  chunk_iterator = dfs.get_chunk_data("cs0", "partitions")

  # При использовании http_dfs читаем с данного сервера
  #chunk_iterator = dfs.get_chunk_data("104.155.8.206", "partitions")
  print("\nThe contents of chunk partitions:")
  for line in chunk_iterator:
    # удаляем символ перевода строки
    print(line[:-1])

# Эту функцию надо реализовать. Функция принимает имя файла и
# возвращает итератор по его строкам.
# Если вы не знаете ничего про итераторы или об их особенностях в Питоне,
# погуглите "python итератор генератор". Вот например
# http://0agr.ru/blog/2011/05/05/advanced-python-iteratory-i-generatory/
def get_file_content(filename):
  chunck_serv = list()
  for f in dfs.files():    
    if f.name == filename:
      for chunk in f.chunks:
        for serv in dfs.chunk_locations():
          if serv.id == chunk:
            chunck_serv.append([serv.chunkserver, chunk])
  for el in chunck_serv:
    data = dfs.get_chunk_data(el[0],el[1])
    for line in data: 
      yield line

# эту функцию надо реализовать. Она принимает название файла с ключами и возвращает
# число
def calculate_sum(keys_filename):
  keys = get_file_content(keys_filename)
  partitions = []
  partitions_gen = get_file_content('/partitions')
  for p in partitions_gen:
    partitions.append(p.split())   
  summ = 0
  for k in keys:
    key = k.strip()
    for p in partitions:
      if (key >= p[0]) and (key <= p[1]):
        gen = get_file_content(p[2])
        for line in gen:
          data = line.split()
          if data:
            if key == data[0]:
              summ += int(data[1])
      
  return summ

demo()
