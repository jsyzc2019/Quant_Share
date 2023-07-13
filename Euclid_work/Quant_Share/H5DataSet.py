# -*- coding: utf-8 -*-
# time: 2023/7/7 18:20
# author: Euclid Jie
# file: H5DataSet.py
# desc: h5文件操作
import gc
import os
import sys
import tempfile
# from itertools import tee
from typing import Optional

import h5py
import numpy as np
import pandas as pd
import psutil
from psutil._common import bytes2human


class H5DataSet:
    def __init__(self, h5FilePath, tab_path='', **kwargs):
        self.f_object_handle = self.__get_h5handle(h5FilePath, mode=kwargs.get('mode', 'r'))
        self.h5FilePath = h5FilePath
        self.tab_path = tab_path
        self.known_data_map = {}
        # 重定向print内容
        current = sys.stdout
        f = open('dirTreeCache', 'w')
        sys.stdout = f
        self.__h5dir(h5FilePath, tab_path)
        sys.stdout = current
        f.close()

        self.known_data = list(self.known_data_map.keys())

    @staticmethod
    def __get_format_path(root_path, *args):
        for sub_path in args:
            if sub_path:
                root_path = os.path.join(root_path, sub_path)
        return root_path.replace("\\", "/")

    @staticmethod
    def __get_format_size(f_object):
        size_b = f_object.size
        if size_b >= 1e9:
            return "{:.2f} G".format(size_b / 1e9)
        elif size_b >= 1e6:
            return "{:.2f} M".format(size_b / 1e6)
        elif size_b >= 1e3:
            return "{:.2f} K".format(size_b / 1e3)
        else:
            return "{:.2f} B".format(size_b)

    def __h5dir(self, h5FilePath, tab_path):
        if isinstance(h5FilePath, h5py.Group):
            f_object = h5FilePath
        else:
            f_object = self.__get_h5handle(h5FilePath)
        print("<dir>  ~{}".format(self.__get_format_path(tab_path)))
        for vv in f_object.attrs.keys():  # 打印属性
            print("%s = %s" % (vv, f_object.attrs[vv]))

        assert f_object, h5py.Dataset | h5py.Group
        Group_list = []

        for k in f_object.keys():
            d = f_object[k]
            if isinstance(d, h5py.Group):
                Group_list.append((d, d.name))
            elif isinstance(d, h5py.Dataset):
                print(
                    "  <file> <{}>  ~{}.{}".format(
                        "%s" % self.__get_format_size(d),
                        self.__get_format_path(d.name),
                        "%s" % d.dtype,
                    )
                )
                for vv in d.attrs.keys():  # 打印属性
                    print("  %s = %s" % (vv, d.attrs[vv]))
                if d.name.split("/")[-1] not in self.known_data_map:
                    self.known_data_map[d.name.split("/")[-1]] = d.name
            else:
                print("??->", d, "Unknown Object!")
                raise TypeError
        for root_path, name in Group_list:
            self.__h5dir(root_path, name)

    @staticmethod
    def h5dir(attrs=False, dir_only=False):
        with open('dirTreeCache', 'r') as f:
            if attrs:
                for line in f.readlines():
                    print(line[:-1])
            else:
                for line in f.readlines():
                    if line.strip().startswith('<dir>' if dir_only else '<'):
                        print(line[:-1])

    @staticmethod
    def __get_h5handle(h5FilePath, mode="r"):
        if not h5FilePath.endswith(".h5"):
            h5FilePath += ".h5"
        assert os.path.exists(h5FilePath)
        f = h5py.File(h5FilePath, mode)
        return f

    def __get_tab_path(self, tab_name):
        if tab_name in self.known_data_map:
            return self.known_data_map[tab_name]
        else:
            raise AttributeError(
                "Attribute '{}' for {} not defined.".format(tab_name, self.h5FilePath)
            )

    def load_h5data(self, tab_name):
        dataSet = self.f_object_handle[self.__get_tab_path(tab_name)]

        if 'view_dtype' in dataSet.attrs.keys():
            return dataSet[:].view(dataSet.attrs['view_dtype'])
        else:
            return dataSet[:]

    @staticmethod
    def format_h5data_type(data_array: np.ndarray):
        data_array_dtype = data_array.dtype.str
        if 'M8' in data_array_dtype:
            return data_array_dtype, data_array.view('u8')
        else:
            return None, data_array

    def __write_array_to_h5dataset(self, h5Object, tableName: str, arrayData: np.ndarray):
        assert h5Object, h5py.Group | h5py.File
        view_dtype, arrayData = self.format_h5data_type(arrayData)
        dataset = h5Object.create_dataset(tableName, data=arrayData)
        if view_dtype is not None:
            dataset.attrs['view_dtype'] = view_dtype

    @classmethod
    def write_pivotDF_to_h5data(cls, h5FilePath, pivotDF: pd.DataFrame, pivotKey: str, rewrite=False):
        if not h5FilePath.endswith(".h5"):
            h5FilePath += ".h5"
        index_array = pivotDF.index.values
        columns_array = pivotDF.columns.values
        data_array = pivotDF.values
        if rewrite:
            mode = 'w'
        else:
            mode = 'a'
        # mode可以是"w",为防止打开一个已存在的h5文件而清除其数据,故使用"a"模式
        with h5py.File(h5FilePath, mode) as file:
            # 创建meta_group，将index_columns写入
            meatDataGroup = file.create_group("meatData")
            cls.__write_array_to_h5dataset(H5DataSet, meatDataGroup, 'index', index_array)
            cls.__write_array_to_h5dataset(H5DataSet, meatDataGroup, 'columns', columns_array)
            # 写入pivot数据
            pivotDataGroup = file.create_group("pivotData")
            cls.__write_array_to_h5dataset(H5DataSet, pivotDataGroup, pivotKey, data_array)
            # 关闭HDF5文件
            file.close()

    @classmethod
    def add_pivotDF_to_h5data(cls, h5FilePath, pivotDF: pd.DataFrame, pivotKey: str, reindex=False):
        if not h5FilePath.endswith(".h5"):
            h5FilePath += ".h5"
        assert os.path.exists(h5FilePath), "{} not exists".format(h5FilePath)
        exists_h5DataSet = H5DataSet(h5FilePath)
        index_array_exists = exists_h5DataSet.load_h5data('index')
        columns_array_exists = exists_h5DataSet.load_h5data('columns')
        exists_h5DataSet.f_object_handle.close()

        index_array_add = pivotDF.index.values
        columns_array_add = pivotDF.columns.values

        assert np.array_equal(index_array_exists, index_array_add)
        assert np.array_equal(columns_array_exists, columns_array_add)
        # todo(Euclid) reindex add pivotDF

        data_array = pivotDF.values
        # mode可以是"w",为防止打开一个已存在的h5文件而清除其数据,故使用"a"模式
        with h5py.File(h5FilePath, 'a') as file:
            cls.__write_array_to_h5dataset(H5DataSet, file, 'pivotData/' + pivotKey, data_array)
            # 关闭HDF5文件
            file.close()

    def load_pivotDF_from_h5data(self, pivotKey: str):
        index_array = self.load_h5data('index')
        columns_array = self.load_h5data('columns')
        data_array = self.load_h5data(pivotKey)
        return pd.DataFrame(data=data_array, index=index_array, columns=columns_array)

    def to_data_frame(self, tableKey):
        return pd.read_hdf(self.h5FilePath, key=tableKey)


class H5DataTS():

    def __init__(self, **kwargs):
        # self.h5FilePath = h5FilePath
        self.h5FilePath = None
        self.h5File_Iterator = None
        self.transform_file_path = None
        self.delete = False
        self.chunk_size: int = kwargs.get('chunk_size', 2000)
        self.key: str = kwargs.get('key', 'a')

    # @classmethod
    def load_h5_data(self, h5FilePath: str, **kwargs):
        try:
            h5File_Iterator = pd.read_hdf(
                h5FilePath,
                chunksize=self.chunk_size,
                key=self.key,
                iterator=True
            )
            self.transform_file_path = h5FilePath
            self.h5File_Iterator = h5File_Iterator
        except TypeError as te:
            self.__type_transform(h5FilePath, **kwargs)

    # @classmethod
    def __type_transform(self, h5FilePath: str, save_path: str = '', **kwargs):
        print(f"Transform file type to 'table'")
        df: pd.DataFrame = pd.read_hdf(h5FilePath, key='a')
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:

            if save_path:
                self.transform_file_path = save_path
            else:
                self.transform_file_path = temp_file.name
                self.delete = True
            df.to_hdf(self.transform_file_path, index=False, format='table', key='a')
            if not kwargs.get('transform_only', False):
                self.load_h5_data(
                    h5FilePath=self.transform_file_path
                )

    def ergodic_process(
            self,
            func,
            break_count: Optional[int] = None,
            reload: bool = True,
            args: Optional = ()):
        assert callable(func)
        # h5File_Iterator = deepcopy(self.h5File_Iterator)
        # h5File_Iterator, self.h5File_Iterator = tee(self.h5File_Iterator, 2)
        assert self.h5File_Iterator is not None
        count = 0
        for ck in self.h5File_Iterator:
            func(ck, *args)
            count += 1
            if break_count and count > break_count:
                break

        if reload:
            # del self.h5File_Iterator
            self.load_h5_data(self.transform_file_path)
        # del ck
        # del h5File_Iterator
        # # 在处理完每个数据块后手动触发垃圾回收
        # gc.collect()

    @staticmethod
    def get_attrs(data, *args):
        for arg in args:
            print(getattr(data, arg))

    def to_list(self):
        return list(self.h5File_Iterator)

    @staticmethod
    def memory_analysis(*args):
        pid = os.getpid()
        # 创建psutil的Process对象
        process = psutil.Process(pid)
        # 获取脚本当前的内存使用量（以字节为单位）
        memory_info = process.memory_info()
        memory_usage = memory_info.rss
        # 将内存使用量转换为更友好的格式
        memory_usage_readable = bytes2human(memory_usage)

        print("Memory Usage:", memory_usage_readable)

    def __del__(self):
        if self.delete and self.transform_file_path is not None:
            os.remove(self.transform_file_path)
            print(f"Delete temporary file {self.transform_file_path}")


if __name__ == "__main__":
    file_path = r'E:\Share\Stk_Data\gm\gmData_history_1m\gmData_history_1m_Y2018_Q3.h5'
    ts = H5DataTS(chunk_size=10000)
    ts.load_h5_data(file_path)
    ts.ergodic_process(
        H5DataTS.get_attrs,
        break_count=None,
        reload=True,
        args=('shape',))
    # h5_lst = ts.to_list()
    # print("======================")
    ts.memory_analysis()
    # ts.ergodic_process(H5DataTS.get_attrs, 'shape')
