# -*- coding: utf-8 -*-
# time: 2023/7/7 18:20
# author: Euclid Jie
# file: H5DataSet.py
# desc: h5文件操作
import os
import h5py
import pandas as pd
import numpy as np
class H5DataSet:
    def __init__(self, h5FilePath, tab_path=''):
        self.f_object_handle = self.__get_h5handle(h5FilePath)
        self.h5FilePath = h5FilePath
        self.tab_path = tab_path
        self.known_data_map = {}
        self.h5dir(out=False)
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

    def __h5dir(self, h5FilePath, tab_path, out):
        if isinstance(h5FilePath, h5py.Group):
            f_object = h5FilePath
        else:
            f_object = self.__get_h5handle(h5FilePath)

        assert f_object, h5py.Dataset | h5py.Group
        Group_list = []
        root_path = tab_path
        if tab_path:
            f_object = f_object[tab_path]
        if out:
            print("<dir>  ~{}".format(self.__get_format_path(root_path, tab_path)))
            for vv in f_object.attrs.keys():  # 打印属性
                print("%s = %s" % (vv, f_object.attrs[vv]))
        for k in f_object.keys():
            d = f_object[k]
            if isinstance(d, h5py.Group):
                Group_list.append((d, os.path.join(root_path, d.name)))
            elif isinstance(d, h5py.Dataset):
                if out:
                    print(
                        "  <file> <{}>  ~{}.{}".format(
                            "%s" % self.__get_format_size(d),
                            self.__get_format_path(root_path, d.name),
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
            self.__h5dir(root_path, name, out)

    def h5dir(self, out=True):
        self.__h5dir(self.h5FilePath, self.tab_path, out)

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
    def format_h5data_type(data_array:np.ndarray):
        data_array_dtype = data_array.dtype.str
        if 'M8' in data_array_dtype:
            return data_array_dtype, data_array.view('u8')
        else:
            return None, data_array

    def __write_array_to_h5dataset(self, h5Object, tableName:str, arrayData:np.ndarray):
        assert h5Object, h5py.Group | h5py.File
        view_dtype, arrayData = self.format_h5data_type(arrayData)
        dataset = h5Object.create_dataset(tableName, data=arrayData)
        if view_dtype is not None:
            dataset.attrs['view_dtype'] = view_dtype

        
    @classmethod
    def write_pivotDF_to_h5data(self, h5FilePath, pivotDF:pd.DataFrame, pivotKey:str,rewrite=False):
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
            self.__write_array_to_h5dataset(H5DataSet, meatDataGroup, 'index', index_array)
            self.__write_array_to_h5dataset(H5DataSet, meatDataGroup, 'columns', columns_array)
            # 写入pivot数据
            pivotDataGroup = file.create_group("pivotData")
            self.__write_array_to_h5dataset(H5DataSet, pivotDataGroup,pivotKey, data_array)
            # 关闭HDF5文件
            file.close()
    
    @classmethod
    def add_pivotDF_to_h5data(self, h5FilePath, pivotDF:pd.DataFrame, pivotKey:str, reindex=False):
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
            self.__write_array_to_h5dataset(H5DataSet, file, 'pivotData/' + pivotKey, data_array)
            # 关闭HDF5文件
            file.close()
    
    def load_pivotDF_from_h5data(self, pivotKey:str):
        index_array = self.load_h5data('index')
        columns_array = self.load_h5data('columns')
        data_array = self.load_h5data(pivotKey)
        return pd.DataFrame(data=data_array, index=index_array, columns=columns_array)

if __name__ == "__main__":
    H5DataSet('rtn_df_1').h5dir()
