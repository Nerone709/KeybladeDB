#!/usr/bin/env python
# coding: utf-8

# # Importo le librerie necessarie

# In[73]:


import pandas as pd
import matplotlib.pyplot as plt
import os
from PIL import Image
import numpy as np
from IPython.display import display


# # Caricamento del dataset

# In[74]:


# Percorso al file CSV
file_path = '../datasets/videogames_sales2016.csv'

# Caricamento del datasets
df = pd.read_csv(file_path)

# Mostra l'intero datasets
display(df)


# # Stampa dei giochi le cui features Publisher e Developer presentano valori diversi

# In[75]:


# Filtra i giochi con valori diversi tra Publisher e Developer
giochi_valori_diversi = df[df['Publisher'] != df['Developer']]

# Stampa dei giochi filtrati
print("Giochi con valori diversi tra Publisher e Developer:")
display(giochi_valori_diversi)


# # Pulizia del dataset
# In questa cella è stata effettuata la pulizia del dataset, nello specifco sono state effettuate le seguenti operazioni:
# 1) Sostituzione dei valori NaN con 'RP' nella colonna 'Rating'
# 2) Sostituzione dei valori NaN con 0.0 nelle colonne 'Critic_Score', 'Critic_Count', 'User_Score', 'User_Count'
# 3) Rimozione delle righe senza nome del gioco
# 4) Stampa delle colonne con valori NaN

# In[76]:


# Pulizia del datasets
if 'Rating' in df.columns:
    df['Rating'] = df['Rating'].fillna('RP')

# Conversione della colonna User_Score in formato numerico
df['User_Score'] = pd.to_numeric(df['User_Score'], errors='coerce')

# Verifica se ci sono valori NaN dopo la conversione
nan_count = df['User_Score'].isnull().sum()
print(f"Numero di valori NaN in 'User_Score' dopo la conversione: {nan_count}")

columns_to_update = ['Critic_Score', 'Critic_Count', 'User_Score', 'User_Count']
for column in columns_to_update:
    if column in df.columns:
        df[column] = df[column].fillna(0.0)

# Rimozione delle righe senza nome del gioco
if 'Name' in df.columns:
    df = df.dropna(subset=['Name'])

# Stampa delle colonne con valori NaN
columns_with_nan = df.columns[df.isnull().any()]
if not columns_with_nan.empty:
    print("Colonne con valori NaN:", list(columns_with_nan))
else:
    print("Non ci sono colonne con valori NaN.")

# Calcolo e stampa dei valori NaN per colonne specifiche
columns_to_check = ['Years_of_Release', 'Publisher', 'Developer']
for column in columns_to_check:
    if column in df.columns:
        nan_count = df[column].isnull().sum()
        print(f"Numero di valori NaN in '{column}': {nan_count}")


# # Stampa del numero di giochi in cui la colonna "Year_of_Release" presenta valore NaN e sostituzione del valore NaN con "Unknown"

# In[77]:


# Numero totale di giochi
numero_giochi = len(df)
print(f"Numero totale di giochi: {numero_giochi}")

# Giochi in cui 'Years_of_Release' non è definito
giochi_senza_anno = df[df['Year_of_Release'].isnull()]
numero_giochi_senza_anno = len(giochi_senza_anno)
print(f"Numero di giochi senza 'Year_of_Release': {numero_giochi_senza_anno}")

# Stampa dei giochi senza 'Years_of_Release'
print("Giochi senza 'Year_of_Release':")
display(giochi_senza_anno)

# Sostituisci i valori NaN nella colonna Year_of_Release con "Unknown"
df['Year_of_Release'] = df['Year_of_Release'].fillna("Unknown")

# Verifica se ci sono ancora valori NaN nella colonna Year_of_Release
nan_count = df['Year_of_Release'].isnull().sum()
print(f"Numero di valori NaN in 'Year_of_Release' dopo la sostituzione: {nan_count}")


# # Sostituzione sia nella colonna Publisher che nella colonna Developer del valore "Unknown" al posto del valore "NaN"

# In[78]:


# Sostituisci i valori NaN con "Unknown" nella colonna Publisher
df['Publisher'] = df['Publisher'].fillna("Unknown")

# Sostituisci i valori NaN con "Unknown" nella colonna Developer
df['Developer'] = df['Developer'].fillna("Unknown")

# Verifica se ci sono ancora giochi con Publisher uguale a NaN
giochi_nan_publisher = df[df['Publisher'].isnull()]
print(f"Numero di giochi con Publisher uguale a NaN: {len(giochi_nan_publisher)}")


# Conta il numero di giochi con valori NaN nella colonna Developer
nan_count_developer = df['Developer'].isnull().sum()

# Stampa il risultato
print(f"Numero di giochi con Developer uguale a NaN: {nan_count_developer}")

# Stampa i giochi filtrati
if not giochi_nan_publisher.empty:
    print("Giochi con Publisher uguale a NaN:")
    display(giochi_nan_publisher)
else:
    print("Non ci sono giochi con Publisher uguale a NaN.")


# # Controllo se sono presenti ancora giochi con valori NaN sia nella colonna Publisher che Developer

# In[79]:


# Filtra i giochi con valori NaN sia in Publisher che in Developer
giochi_nan_aggiornati = df[df['Publisher'].isnull() & df['Developer'].isnull()]

# Numero di giochi filtrati
numero_giochi_nan_aggiornati = len(giochi_nan_aggiornati)
print(f"Numero di giochi con valori NaN sia in Publisher che in Developer dopo gli aggiornamenti: {numero_giochi_nan_aggiornati}")

# Stampa dei giochi filtrati
print("Giochi con valori NaN sia in Publisher che in Developer dopo gli aggiornamenti:")
display(giochi_nan_aggiornati)


# # Stampa del dataset aggiornato con tutte le modifiche effettuate

# In[80]:


# Stampa dell'intero datasets aggiornato
print("Dataset aggiornato con tutte le modifiche:")
display(df)


# # Controllo se sono presenti ancora colonne nel Dataset che presentano valori NaN

# In[81]:


# Controlla le colonne con valori NaN
columns_with_nan = df.columns[df.isnull().any()]

# Stampa le colonne con valori NaN
if not columns_with_nan.empty:
    print("Colonne con valori NaN:", list(columns_with_nan))
else:
    print("Non ci sono colonne con valori NaN.")


# # Calcolo del valore massimo di Critic_Score, Critic_Count, User_Score e User_Count utilizzato per confrontarlo con eventuali risultati ottenuti nei boxplot in modo da verificare eventuale coerenza dei dati

# In[82]:


# Calcolo e stampa del valore massimo per ogni colonna
columns_to_check = ['Critic_Score', 'Critic_Count', 'User_Score', 'User_Count']
for column in columns_to_check:
    max_value = df[column].max()
    print(f"Il valore massimo di {column} è: {max_value}")

