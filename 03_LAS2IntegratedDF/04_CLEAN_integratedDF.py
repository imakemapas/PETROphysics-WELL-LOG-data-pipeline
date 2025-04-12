# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 18:44:45 2025
@author: Theresa Rocco Pereira Barbosa
"""

import os
import pandas as pd
from tqdm import tqdm

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================
INPUT_DIR    = r"C:\Users\Theresa\OneDrive\PESQUISA\LabMeg_Exxon\PMP_BC_WELL_LOGGING\03_LAS2IntegratedDF\DF_output_03"
OUTPUT_PATH  = r"C:\Users\Theresa\OneDrive\PESQUISA\LabMeg_Exxon\PMP_BC_WELL_LOGGING\03_LAS2IntegratedDF\DF_output_04\integrated_data.csv"
OUTPUT_PATH2 = r"C:\Users\Theresa\OneDrive\PESQUISA\LabMeg_Exxon\PMP_BC_WELL_LOGGING\03_LAS2IntegratedDF\DF_output_04\cleaned_data.csv"

# Lista das colunas desejadas (priorizando as mais completas)
TARGET_COLUMNS = [
    'ID',
    'DEPTH',            # Profundidade
    'SP',               #Potencial Espontâneo
    'GR',               # Raios Gama
    'NEUT',             # Porosidade Neutrônica
    'CALI',             # Caliper
    'RES_MEDIUM',       # Resistividade Média
    'SONIC_INT',        # Sônico Integrado
    'TEMP_OUT',         # Temperatura de Saída
    'RES_DEEP',         # Resistividade Profunda
    'SONIC',            # Sônico
    'DEN',              # Densidade
    'DEN_CORR',         # Densidade Corrigida
    'RES_SHALLOW',      # Resistividade Rasa
    'RLAT',             # Resistividade Lateral
]

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================
def combine_and_filter_logs(input_dir, output_full_path, output_filtered_path):
    """Combina e filtra os dados de poço"""
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    combined_df = pd.DataFrame()
    
    print(f"Processando {len(csv_files)} poços...")
    
    # Etapa 1: Consolidação
    for filename in tqdm(csv_files, desc="Consolidando dados"):
        file_path = os.path.join(input_dir, filename)
        df = pd.read_csv(file_path)
        well_id = os.path.splitext(filename)[0]
        df['ID'] = well_id
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    
    # Ordenação geológica
    combined_df.sort_values(by=['ID', 'DEPTH'], inplace=True)
    combined_df.to_csv(output_full_path, index=False)
    
    # Etapa 2: Filtragem das colunas-alvo
    print("\nFiltrando colunas prioritárias...")
    filtered_df = combined_df.reindex(columns=TARGET_COLUMNS)
    
    # Remove colunas que não existiam no DataFrame original
    existing_cols = [col for col in TARGET_COLUMNS if col in combined_df.columns]
    filtered_df = filtered_df[existing_cols]
    
    # Verificação final
    missing_cols = set(TARGET_COLUMNS) - set(existing_cols)
    if missing_cols:
        print(f"Aviso: Colunas não encontradas em nenhum poço: {missing_cols}")
    
    # Salva o dataset filtrado
    filtered_df.to_csv(output_filtered_path, index=False)
    
    # Relatório
    print("\n✅ Processamento concluído:")
    for col in existing_cols:
        print(f"  → {col}")
    
    return combined_df, filtered_df

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    print("==============================================")
    print("  PROCESSAMENTO FINAL DE DADOS DE POÇOS")
    print("==============================================")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    full_df, clean_df = combine_and_filter_logs(
        INPUT_DIR,
        OUTPUT_PATH,
        OUTPUT_PATH2
    )

df_clean = pd.read_csv(OUTPUT_PATH2)

for col in df_clean.columns:
    print(col)