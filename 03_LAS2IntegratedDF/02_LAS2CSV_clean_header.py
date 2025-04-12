# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 17:19:53 2025

@author: Theresa Rocco Pereira Barbosa
"""

import os
import lasio
import pandas as pd
from tqdm import tqdm

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

INPUT_DIR  = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/03_LAS2IntegratedDF/LAS_input"
OUTPUT_DIR = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/03_LAS2IntegratedDF/DF_output_02"

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================

def las_to_csv(las_path, output_path):
    """Converte um arquivo LAS para CSV mantendo os nomes originais"""
    # Cria diretório de saída se não existir
    os.makedirs(output_path, exist_ok=True)
    
    try:
        las = lasio.read(las_path)
        data = {curve.mnemonic: las[curve.mnemonic] for curve in las.curves}
        pd.DataFrame(data).to_csv(output_path, index=False)
        return True
    except Exception as e:
        print(f"❌ Erro em {os.path.basename(las_path)}: {str(e)}")
        return False

def process_directory(input_dir, output_dir):
    """Processa todos os arquivos LAS de um diretório"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    las_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.las')]
    print(f"🔍 Encontrados {len(las_files)} arquivos LAS para conversão")
    
    success_count = 0
    for las_file in tqdm(las_files, desc="Convertendo"):
        input_path = os.path.join(input_dir, las_file)
        output_path = os.path.join(output_dir, f"{os.path.splitext(las_file)[0]}.csv")
        
        if las_to_csv(input_path, output_path):
            success_count += 1
    
    print(f"\n✅ Conversão concluída: {success_count}/{len(las_files)} arquivos processados com sucesso")
    if success_count < len(las_files):
        print("⚠️ Alguns arquivos tiveram erros ")
        
# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    print("===========================================")
    print("  CONVERSOR EM MASSA - LAS PARA CSV")
    print("===========================================")
    
    process_directory(INPUT_DIR, OUTPUT_DIR)