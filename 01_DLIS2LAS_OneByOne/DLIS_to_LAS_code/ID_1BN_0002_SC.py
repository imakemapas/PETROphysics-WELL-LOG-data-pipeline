# -*- coding: utf-8 -*-
"""
Autor: Theresa Rocco Pereira Barbosa
Data: 09/04/2025

Script para conversão de arquivos DLIS para LAS,
combinando múltiplos Logical Files em um único arquivo LAS.

TUTORIAL DE USO:
1. Configure os caminhos base e nome do arquivo nas variáveis no início do script
2. Execute o script para gerar o arquivo LAS combinado
3. O arquivo de saída será salvo no diretório LAS_output

Baseado no tutorial de Andy McDonald
* https://www.andymcdonald.scot/loading-well-log-data-from-dlis-using-python-9d48df9a23e2
* https://www.andymcdonald.scot/converting-well-logging-data-from-dlis-files-to-las-file-format
"""

# =============================================================================
# IMPORTAÇÃO BIBLIOTECAS
# =============================================================================

from dlisio import dlis # DLISIO nos permite ler e trabalhar com o conteúdo dos arquivos DLIS
import lasio            # LASIO nos permite trabalhar com arquivos LAS
import numpy as np
import os

# =============================================================================
# CONFIGURAÇÕES INICIAIS - Área para ajustes pelo usuário
# =============================================================================

BASE_PATH = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/01_DLIS2LAS_OneByOne/"
DLIS_FILE_NAME = "1BN__0002__SC_1BN__0002__SC"

DLIS_PATH = os.path.join(BASE_PATH, "DLIS_input", f"{DLIS_FILE_NAME}.dlis")
print(DLIS_PATH)

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def format_simple(name):
    """
    Formata o nome do arquivo para um padrão mais legível
    
    """
    parts = name.split('__')
    num_letras = parts[0]  # '1MB'
    numero = parts[1]      # '0001'
    letras = parts[2].split('_')[0]  # 'SC'
    
    prefix_num = ''.join([c for c in num_letras if c.isdigit()])
    prefix_let = ''.join([c for c in num_letras if c.isalpha()])
    
    return f"{prefix_num}_{prefix_let}_{int(numero)}_{letras}"

# Gera nome do arquivo LAS de saída
LAS_FILE_NAME = format_simple(DLIS_FILE_NAME)
LAS_PATH = os.path.join(BASE_PATH, "LAS_output", f"{LAS_FILE_NAME}.las")
print(f"Caminho LAS: {LAS_PATH}")

# =============================================================================
# LEITURA E ANÁLISE DOS ARQUIVOS DLIS
# =============================================================================

# Arquivos DLIS podem conter múltiplos arquivos lógicos, representando diferentes 
# passagens de registro ou níveis de processamento.

# Carrega o arquivo DLIS e separa os Logical Files
f, *tail = dlis.load(DLIS_PATH)

# Lista com todos os Logical Files para processamento posterior
all_files = [f] + tail

print("\nESTRUTURA DO ARQUIVO DLIS:")
for i, lf in enumerate(all_files, start=1):
    print(f"\n=== Logical File {i} ===")
    print(lf.describe())  # Método describe() mostra estrutura resumida
    
# =============================================================================
# PROCESSAMENTO DOS FRAMES E CANAIS
# =============================================================================

print("\nDETALHES DOS FRAMES:")
for i, lf in enumerate(all_files, start=1):
    print(f"\n=== Logical File {i} ===")
    print("-" * 40)
    
    for frame in lf.frames:
        #  Verificar se existe canal de profundidade
        depth_channel = next((ch for ch in frame.channels if ch.name == frame.index), None)
        depth_units = depth_channel.units if depth_channel else "N/A"
        
        #  Formatação consistente da saída
        print(f'Frame Name: \t\t {frame.name}')
        print(f'Index Type: \t\t {frame.index_type}')
        print(f'Depth Interval: \t {frame.index_min} - {frame.index_max} {depth_units}')
        print(f'Depth Spacing: \t\t {frame.spacing} {depth_units}')
        print(f'Direction: \t\t {frame.direction}')
        print(f'Num of Channels: \t {len(frame.channels)}')
        
        #  Lista formatada dos canais
        print("Channel Names:")
        for channel in frame.channels:
            print(f'  - {channel.name} ({channel.units or "No unit"})')
        print()
        
# Cada frame tem um número de propriedades.
# Eles são como um "bloco" de registros com estrutura comum — tipo uma planilha onde 
# cada linha é uma profundidade e as colunas são os canais.

# =============================================================================
# FUNÇÃO PRINCIPAL DE CONVERSÃO
# =============================================================================
def load_and_combine_dlis(dlis_path, output_path):
    """
    Função principal que carrega arquivos DLIS e exporta para LAS, com:
    - Conversão automática de unidades (pés para metros)
    - Filtro de canais DUMM
    - Preservação de metadados
    
    Parâmetros:
        dlis_path (str): Caminho para o arquivo DLIS de entrada
        output_path (str): Caminho para o arquivo LAS de saída
    
    Retorno:
        lasio.LASFile: Objeto LAS gerado
    """
    # 1. Carrega todos os Logical Files
    try:
        f, *tail = dlis.load(dlis_path)
        all_files = [f] + tail
    except Exception as e:
        print(f"Erro ao carregar DLIS: {e}")
        raise
    
    # 2. Extrai metadados básicos
    origin = f.origins[0]
    las = lasio.LASFile()
    las.well['WELL'] = lasio.HeaderItem('WELL', value=origin.well_name)
    las.well['FLD'] = lasio.HeaderItem('FLD', value=origin.field_name or '')
    las.well['COMP'] = lasio.HeaderItem('COMP', value=origin.company or '')
    
    # 3. Determina intervalo de profundidade global com conversão de unidades
    global_min = float('inf')
    global_max = float('-inf')
    
    # Primeiro passada: encontra os valores mínimos/máximos já convertidos para metros
    for lf in all_files:
        for frame in lf.frames:
            depth_channel = next((ch for ch in frame.channels if ch.name == frame.index), None)
            if depth_channel:
                # Verifica unidades e converte se necessário
                if depth_channel.units and 'ft' in depth_channel.units.lower():
                    frame_min = frame.index_min * 0.3048  # Converte pés para metros
                    frame_max = frame.index_max * 0.3048
                else:
                    frame_min = frame.index_min
                    frame_max = frame.index_max
                
                global_min = min(global_min, frame_min)
                global_max = max(global_max, frame_max)
    
    # Define o espaçamento baseado nas unidades predominantes (0.2m ou 0.5ft convertido)
    step = 0.2  # AJUSTAR PARA O SEU CASO!!!!!!!!
    depth_global = np.arange(global_min, global_max + step, step)
    las.append_curve('DEPT', depth_global, unit='m', descr='Depth')
    
    # 4. Coleta todos os canais únicos (excluindo DUMM e canais de índice)
    all_channels = {'DEPT': depth_global}
    channel_units = {}
    
    for lf in all_files:
        for frame in lf.frames:
            for channel in frame.channels:
                channel_name = channel.name
                # Filtra canais indesejados
                if (not channel_name.startswith('INDEX') and 
                    not channel_name == 'DUMM' and 
                    not channel_name.startswith('DUMM_')):
                    
                    if channel_name not in all_channels:
                        all_channels[channel_name] = np.full_like(depth_global, np.nan)
                        # Mantém as unidades originais exceto para profundidade
                        channel_units[channel_name] = channel.units or ''
    
    # 5. Preenche os dados com conversão de unidades quando necessário
    for lf in all_files:
        for frame in lf.frames:
            depth_channel = next((ch for ch in frame.channels if ch.name == frame.index), None)
            if not depth_channel:
                continue
                
            # Verifica se precisa converter de pés para metros
            convert_to_meters = depth_channel.units and 'ft' in depth_channel.units.lower()
            depth_values = depth_channel.curves()
            if convert_to_meters:
                depth_values = depth_values * 0.3048  # Converte valores de profundidade
            
            for channel in frame.channels:
                channel_name = channel.name
                # Aplica mesmo filtro que na coleta
                if (not channel_name.startswith('INDEX') and 
                    not channel_name == 'DUMM' and 
                    not channel_name.startswith('DUMM_')):
                    
                    channel_data = channel.curves()
                    # Interpola para o grid global de profundidade (já em metros)
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
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        las.write(output_path)
        print(f"\nArquivo LAS combinado salvo em: {output_path}")
        
        # Mostra estatísticas do arquivo gerado
        print("\nCONTEUDO DO ARQUIVO GERADO:")
        print(f"Intervalo de profundidade: {global_min:.2f} - {global_max:.2f} m")
        print(f"Canais incluídos: {list(all_channels.keys())}")
        
    except Exception as e:
        print(f"Erro ao salvar arquivo LAS: {e}")
        raise
    
    return las

# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================
las = load_and_combine_dlis(DLIS_PATH, LAS_PATH)
