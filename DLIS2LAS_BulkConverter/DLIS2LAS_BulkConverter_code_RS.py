# -*- coding: utf-8 -*-
"""
Created on Fri Apr 11 17:23:34 2025

@author: Theresa
"""

import os
from dlisio import dlis
import lasio
import numpy as np

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

BASE_PATH = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/DLIS2LAS_BulkConverter/"
DLIS_INPUT_DIR = os.path.join(BASE_PATH, "DLIS_input/RS")
LAS_OUTPUT_DIR = os.path.join(BASE_PATH, "LAS_output")

# =============================================================================
# FUNÇÕES AUXILIARES 
# =============================================================================

def format_simple(name):

    # Padrão 1: Nomes com 3 caracteres (1MB__0001__SC)
    if len(name.split('__')[0]) == 3:  # Verifica se as iniciais têm 3 caracteres
        parts = name.split('__')
        num_letras = parts[0]          
        numero = parts[1]            
    # Padrão 2: Nomes com 4 caracteres (1RCH_0001__SC)
    else:
        parts = name.split('_')
        num_letras = parts[0]         
        numero = parts[1]              
    
    # Extrai dígitos e letras iniciais
    prefix_num = ''.join([c for c in num_letras if c.isdigit()])  
    prefix_let = ''.join([c for c in num_letras if c.isalpha()])  
    
    return f"{prefix_num}_{prefix_let}_{int(numero)}_RS"  

# =============================================================================
# FUNÇÃO PARA LISTAR ARQUIVOS DLIS
# =============================================================================

def list_dlis_files(dlis_dir):
    """Lista todos os arquivos .dlis no diretório especificado"""
    dlis_files = []
    for file in os.listdir(dlis_dir):
        if file.endswith(".dlis"):
            # Remove a extensão .dlis para obter o nome base
            base_name = os.path.splitext(file)[0]
            dlis_files.append(base_name)
    return dlis_files

# =============================================================================
# PROCESSAMENTO EM LOTE
# =============================================================================

def process_all_dlis_files():
    """Processa todos os arquivos DLIS na pasta de entrada"""
    # Cria diretório de saída se não existir
    os.makedirs(LAS_OUTPUT_DIR, exist_ok=True)
    
    # Obtém lista de todos os arquivos DLIS
    dlis_files = list_dlis_files(DLIS_INPUT_DIR)
    
    print(f"\nEncontrados {len(dlis_files)} arquivos DLIS para processar:")
    for i, file in enumerate(dlis_files, 1):
        print(f"{i}. {file}")
    
    # Processa cada arquivo
    for dlis_file_name in dlis_files:
        try:
            print(f"\n{'='*60}")
            print(f"Processando arquivo: {dlis_file_name}")
            print(f"{'='*60}")
            
            # Monta caminhos completos
            dlis_path = os.path.join(DLIS_INPUT_DIR, f"{dlis_file_name}.dlis")
            las_file_name = format_simple(dlis_file_name)
            las_path = os.path.join(LAS_OUTPUT_DIR, f"{las_file_name}.las")
            
            # Processa o arquivo DLIS individual
            process_single_dlis(dlis_path, las_path)
            
        except Exception as e:
            print(f"\n⚠️ Erro ao processar {dlis_file_name}: {e}")
            continue

# =============================================================================
# FUNÇÃO DE PROCESSAMENTO INDIVIDUAL (sua função original adaptada)
# =============================================================================

def process_single_dlis(dlis_path, las_path):
    """Processa um único arquivo DLIS e salva como LAS"""
    # 1. Carrega todos os Logical Files
    f, *tail = dlis.load(dlis_path)
    all_files = [f] + tail
    
    # 2. Extrai metadados básicos
    origin = f.origins[0]
    las = lasio.LASFile()
    las.well['WELL'] = lasio.HeaderItem('WELL', value=origin.well_name)
    las.well['FLD'] = lasio.HeaderItem('FLD', value=origin.field_name or '')
    las.well['COMP'] = lasio.HeaderItem('COMP', value=origin.company or '')
    
    # 3. Determina intervalo de profundidade global com conversão de unidades
    global_min = float('inf')
    global_max = float('-inf')
    
    for lf in all_files:
        for frame in lf.frames:
            depth_channel = next((ch for ch in frame.channels if ch.name == frame.index), None)
            if depth_channel:
                if depth_channel.units and 'ft' in depth_channel.units.lower():
                    frame_min = frame.index_min * 0.3048
                    frame_max = frame.index_max * 0.3048
                else:
                    frame_min = frame.index_min
                    frame_max = frame.index_max
                
                global_min = min(global_min, frame_min)
                global_max = max(global_max, frame_max)
    
    step = 0.2
    depth_global = np.arange(global_min, global_max + step, step)
    las.append_curve('DEPT', depth_global, unit='m', descr='Depth')
    
    # 4. Coleta todos os canais únicos (excluindo DUMM e canais de índice)
    all_channels = {'DEPT': depth_global}
    channel_units = {}
    
    for lf in all_files:
        for frame in lf.frames:
            for channel in frame.channels:
                channel_name = channel.name
                if (not channel_name.startswith('INDEX')) and (not channel_name == 'DUMM'):
                    if channel_name not in all_channels:
                        all_channels[channel_name] = np.full_like(depth_global, np.nan)
                        channel_units[channel_name] = channel.units or ''
    
    # 5. Preenche os dados com conversão de unidades quando necessário
    for lf in all_files:
        for frame in lf.frames:
            depth_channel = next((ch for ch in frame.channels if ch.name == frame.index), None)
            if not depth_channel:
                continue
                
            convert_to_meters = depth_channel.units and 'ft' in depth_channel.units.lower()
            depth_values = depth_channel.curves()
            if convert_to_meters:
                depth_values = depth_values * 0.3048
            
            for channel in frame.channels:
                channel_name = channel.name
                if (not channel_name.startswith('INDEX')) and (not channel_name == 'DUMM'):
                    channel_data = channel.curves()
                    all_channels[channel_name] = np.interp(
                        depth_global,
                        depth_values,
                        channel_data,
                        left=np.nan,
                        right=np.nan
                    )
    
    # 6. Adiciona todos os canais ao LAS
    for name, values in all_channels.items():
        if name != 'DEPT':
            las.append_curve(name, values, unit=channel_units.get(name, ''), descr=name)
    
    # 7. Salva o arquivo
    las.write(las_path)
    print(f"✅ Arquivo LAS salvo em: {las_path}")
    print(f"   Intervalo de profundidade: {global_min:.2f} - {global_max:.2f} m")
    print(f"   Canais incluídos: {list(all_channels.keys())}")

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    print("\nINICIANDO PROCESSAMENTO EM LOTE DE ARQUIVOS DLIS")
    print(f"Diretório de entrada: {DLIS_INPUT_DIR}")
    print(f"Diretório de saída: {LAS_OUTPUT_DIR}")
    
    process_all_dlis_files()
    
    print("\nPROCESSAMENTO CONCLUÍDO!")