import pandas as pd
import streamlit as st

st.title("Opendamir 2019")


def load_data():
    temp = pd.read_csv("data/opendamir_2019.csv")
    ir_nat_v = (pd.read_csv("data/IR_NAT_V.csv", sep=";")
                .rename(columns={"PRS_NAT_LIB": "Libelle prestations"}))
    pse_act_cat = (pd.read_csv("data/pse_act_cat.csv", sep="\t")
                   .rename(columns={"Libellé Catégorie de l'Exécutant": "Libelle executant"}))
    temp = pd.merge(temp, ir_nat_v[["PRS_NAT", "Libelle prestations"]], on="PRS_NAT", how="left")
    temp = pd.merge(temp, pse_act_cat[["PSE_ACT_CAT", "Libelle executant"]], on="PSE_ACT_CAT", how="left")
    return temp.loc[:, ["Libelle prestations", "Libelle executant", "PRS_PAI_MNT", "PRS_REM_MNT"]]


df = load_data()

option_presta = st.sidebar.multiselect('Select act',
                                       df['Libelle prestations'].unique())
option_exec = st.sidebar.multiselect('Select category',
                                     df['Libelle executant'].unique())

st.subheader('Expenditure and amount reimbursed')
chart_data = df.loc[(df["Libelle prestations"].isin(option_presta)) & (df['Libelle executant'].isin(option_exec)),
                    ["PRS_PAI_MNT", "PRS_REM_MNT"]].stack().reset_index().rename(
    columns={"level_1": "Variable", 0: "Amount"})
chart_data = chart_data.loc[:, ["Variable", "Amount"]]
chart_data = chart_data.set_index("Variable")
st.bar_chart(chart_data, width=1)

df.loc[(df["Libelle prestations"].isin(option_presta)) & (df['Libelle executant'].isin(option_exec)), :]

'You selected: ', option_presta, option_exec
