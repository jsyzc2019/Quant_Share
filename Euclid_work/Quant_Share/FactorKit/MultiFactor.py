import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm

warnings.filterwarnings("ignore")
import seaborn as sns
from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo
import matplotlib.pylab as plt
from .FactorCalc import FactorBase
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix


class MultiAnalyzer(FactorBase):
    def __int__(self):
        pass

    @staticmethod
    def prepare(obj, names: list[str]) -> list[pd.DataFrame]:
        with tqdm(names) as t:
            res = []
            for name in t:
                t.set_description(f"Preparing {name}")
                res.append(getattr(obj, name))
        return res

    @staticmethod
    def retrive_ticker(ticker, factor_lst, names=None):
        res = pd.concat([df[ticker] for df in factor_lst], axis=1)
        if names is None:
            names = []
            suffix = 1
            for df in factor_lst:
                try:
                    name = df.name
                    names.append(name)
                except AttributeError:
                    names.append("unknown factor " + str(suffix))
                    suffix += 1
        res.columns = names
        return res

    @staticmethod
    def analysis(mul: pd.DataFrame, num, rotation="varimax"):
        assert 0 < num < mul.shape[1]
        chi_square_value, p_value = calculate_bartlett_sphericity(mul)
        if p_value <= 0.05:
            print(
                f"p-Val:{p_value}: There is a certain correlation between the variables"
            )
        else:
            print(
                f"p-Val:{p_value}: There is NOT a certain correlation between the variables"
            )

        kmo_all, kmo_model = calculate_kmo(mul)
        if kmo_model > 0.6:
            print(
                f"KMO:{kmo_model}: Value of KMO larger than 0.6 is considered adequate"
            )
        else:
            print(
                f"KMO:{kmo_model}: Value of KMO less than 0.6 is considered inadequate"
            )

        faa = FactorAnalyzer(mul.shape[1], rotation=None)
        faa.fit(mul)
        # 得到特征值ev、特征向量v
        ev, v = faa.get_eigenvalues()

        plt.figure(figsize=(8, 8))
        # 同样的数据绘制散点图和折线图
        plt.scatter(range(1, mul.shape[1] + 1), ev)
        plt.plot(range(1, mul.shape[1] + 1), ev)

        # 显示图的标题和xy轴的名字
        # 最好使用英文，中文可能乱码
        plt.title("Scree Plot")
        plt.xlabel("Factors")
        plt.ylabel("Eigenvalue")

        plt.grid()  # 显示网格
        plt.show()  # 显示图形

        faa_sub = FactorAnalyzer(num, rotation=rotation)
        faa_sub.fit(mul)

        with pd.option_context("expand_frame_repr", False, "display.max_rows", None):
            print("因子方差".center(30, "-"))
            print(
                pd.DataFrame(
                    faa_sub.get_communalities(), index=mul.columns, columns=["Std"]
                )
            )
            print("因子特征值".center(30, "-"))
            print(
                pd.DataFrame(
                    faa_sub.get_eigenvalues(),
                    index=[
                        "original_eigen_values(原始特征值)",
                        "common_factor_eigen_values(公共因子特征值)",
                    ],
                    columns=mul.columns,
                )
            )
            print("成分矩阵".center(30, "-"))
            print(pd.DataFrame(faa_sub.loadings_, index=mul.columns))
            print("因子贡献率".center(30, "-"))
            print(
                pd.DataFrame(
                    faa_sub.get_factor_variance(),
                    index=["SS Loadings", "Proportion Var", "Cumulative Var"],
                )
            )

        df = pd.DataFrame(np.abs(faa_sub.loadings_), index=mul.columns)
        plt.figure(figsize=(8, 8))
        ax = sns.heatmap(df, annot=True, cmap="BuPu")
        # 设置y轴字体大小
        ax.yaxis.set_tick_params(labelsize=15)
        plt.title("Factor Analysis", fontsize="xx-large")
        # 设置y轴标签
        plt.ylabel("Sepal Width", fontsize="xx-large")
        # 显示图片
        plt.show()

        return faa_sub

    @staticmethod
    def RFsignal(mul: pd.DataFrame, signal=None, **kwargs):
        assert "chgPct" in mul.columns or signal is not None
        mul["signal"] = mul["chgPct"].shift(-1)
        mul[mul["signal"] > 0] = 1
        mul[mul["signal"] <= 0] = 0
        mul.dropna(inplace=True, how="any", axis=0)
        X = mul.drop(["signal"], axis=1)
        y = mul["signal"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        clf = RandomForestClassifier(
            bootstrap=True,
            class_weight=None,
            criterion="gini",
            max_depth=None,
            max_features="auto",
            max_leaf_nodes=None,
            min_samples_leaf=1,
            min_samples_split=2,
            min_weight_fraction_leaf=0.0,
            n_estimators=10,
            n_jobs=4,
            oob_score=False,
            random_state=None,
            verbose=0,
            warm_start=False,
        )
        clf.fit(X_train, y_train)
        print(f"Train Acc: {clf.score(X_train, y_train)}")
        print(f"Test Acc: {clf.score(X_test, y_test)}")

        C1 = confusion_matrix(y, clf.predict(X))
        plt.figure()
        h = sns.heatmap(
            C1,
            fmt="g",
            cmap="Blues",
            annot=True,
            cbar=False,
            xticklabels=[0, 1],
            yticklabels=[0, 1],
        )
        cb = h.figure.colorbar(h.collections[0])  # 显示colorbar
        cb.ax.tick_params(labelsize=12)  # 设置colorbar刻度字体大小。
        plt.title("Confusion Matrix")
        plt.xlabel("Predicted Labels")
        plt.ylabel("True Labels")
        plt.show()

        factor_weight = pd.DataFrame(
            {"features": list(X.columns), "importance": clf.feature_importances_}
        ).sort_values(
            # 这里根据重要程度降序排列，一遍遍找到重要性最高的特征
            by="importance",
            ascending=False,
        )
        plt.figure(figsize=(6, 4), dpi=128)
        sns.barplot(
            y=factor_weight["features"], x=factor_weight["importance"], orient="h"
        )
        plt.xlabel("Importance")
        plt.ylabel("Factor names")
        plt.xticks(fontsize=10, rotation=35)
        plt.title("Feature importance")
        plt.show()
