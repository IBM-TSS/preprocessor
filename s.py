import pandas as pd


csv = pd.read_excel('STD_Fallas_1er_nivel_may_jun.xlsx')

print(set(csv['failure']))