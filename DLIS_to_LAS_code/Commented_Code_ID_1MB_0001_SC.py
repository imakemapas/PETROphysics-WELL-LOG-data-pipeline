# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 19:35:47 2025

@author: Theresa Rocco Pereira Barbosa
"""

# Este código foi baseado no tutorial de Andy McDonald
# https://www.andymcdonald.scot/loading-well-log-data-from-dlis-using-python-9d48df9a23e2
# https://www.andymcdonald.scot/converting-well-logging-data-from-dlis-files-to-las-file-format

#pip install dlisio
#pip install lasio
#pip install pandas

# DLISIO nos permite ler e trabalhar com o conteúdo dos arquivos DLIS
# LASIO nos permite trabalhar com arquivos LAS

from dlisio import dlis
import lasio
import pandas as pd
pd.set_option('display.max_row', 50)

# Arquivos DLIS podem conter múltiplos arquivos lógicos, representando diferentes passagens de registro ou níveis de processamento.
# Para lidar com isso, separamos o conteúdo em duas variáveis:
# f: recebe o primeiro arquivo lógico
# *tail: recebe os demais arquivos lógicos

f, *tail = dlis.load("C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/original_data/dlis_data/1MB__0001__SC_1MB__0001__SC.dlis")

# Podemos verificar o conteúdo chamando os nomes
# f retorna, por exemplo, LogicalFile(1MB  0001  SCBRA1MB  0001  SC1)
# tail retorna uma lista vazia se não houver outros arquivos lógicos no DLIS.

print(f)
print(tail)

# Para visualizar o conteúdo de alto nível do arquivo, podemos usar o método .describe(). 
# Ele retorna informações sobre o número de quadros, canais e objetos no arquivo lógico.

f.describe() #  Nesse exemplo temos 4 channels e 1 frame!

# Em seguida, obtemos o resumo detalhando as origens desse conjunto de dados.
# Usaremos essas informações quando criarmos nosso arquivo LAS.

origin, *origin_tail = f.origins
print(len(origin_tail))
origin.describe()

# Os frames em um arquivo DLIS podem representar diferentes passagens de registro ou diferentes estágios de dados, 
# como medições brutas de registro de poço para interpretações petrofísicas ou dados processados.
# Cada frame tem um número de propriedades.
# Eles são como um "bloco" de registros com estrutura comum — tipo uma planilha onde 
# cada linha é uma profundidade e as colunas são os canais.

for frame in f.frames:
    
    # Search through the channels for the index and obtain the units
    for channel in frame.channels:
        if channel.name == frame.index:
            depth_units = channel.units
    
    print(f'Frame Name: \t\t {frame.name}')
    print(f'Index Type: \t\t {frame.index_type}')
    print(f'Depth Interval: \t {frame.index_min} - {frame.index_max} {depth_units}')
    print(f'Depth Spacing: \t\t {frame.spacing} {depth_units}')
    print(f'Direction: \t\t {frame.direction}')
    print(f'Num of Channels: \t {len(frame.channels)}')
    print(f'Channel Names: \t\t {str(frame.channels)}')
    print('\n\n')

# Frame Name: 	  [0,1,IES/BOREHOLE-DEPTH/1/.20/M]
# Index Type: 	  BOREHOLE-DEPTH    # Tipo de índice usado para registrar os dados — neste caso, profundidade medida ao longo do poço.
# Depth Interval:  125.0 - 2069.8 m # Intervalo de profundidade para os dados neste frame.
# Depth Spacing:   0.2 m            # Os dados foram registrados a cada 0,2 metros — essa é a resolução do perfil.
# Direction: 	  INCREASING        # A profundidade aumenta com o tempo — o registro foi feito de cima para baixo.
# Num of Channels: 4                # Existem 5 canais registrados neste frame.
# Channel Names:   [Channel(INDEX3652), Channel(RILD), Channel(SP), Channel(CIL)]

# --------- #

# Criando um objeto de arquivo LAS em branco para armazenar extração do DLIS

las_file = lasio.LASFile()

# Confirmar que ele está em branco

las_file.curves

# Extraindo informaçoes importantes do DLIS para o LAS
# Neste exemplo, extrairemos o nome do poço, o nome do campo e a empresa operadora.

well_name  = origin.well_name
field_name = origin.field_name
operator   = origin.company

# Com as informações-chave extraídas, podemos começar a preencher o cabeçalho do arquivo LAS.
# Isso é feito acessando as propriedades do LAS e definindo os 'HeaderItem' com os novos valores.

las_file.well['WELL'] = lasio.HeaderItem('WELL', value = well_name)
las_file.well['FLD'] = lasio.HeaderItem('FLD', value = field_name)
las_file.well['COMP'] = lasio.HeaderItem('COMP', value = operator)

# Adicionamos a data manualmente, pois essa propriedade não parece ser exposta pelo DLISIO.
# Esta em 'origin.describe()'.

#las_file.well['DATE'] = '2019-11-18'

# Selecionado as logging curves observadas no frame

columns_to_extract = ['INDEX3652', 'RILD', 'CIL', 'SP']

# Agora que preparamos nosso arquivo LAS e extraímos os dados de cabeçalho do arquivo DLIS, 
# podemos percorrer os channels no frame no DLIS.


frame = f.frames[0]

for channel in frame.channels:
    # If the channel name is in the list of channels to extract
    if channel.name in columns_to_extract:
        curves = channel.curves()

        # If the channel name is 'INDEX...', convert to 'DEPT' 
        if channel.name == 'INDEX3652':
            channel_name = 'DEPT'
            description = 'DEPTH'
            # If the units are 0.1 in then convert to metres
            if channel.units == '0.1 in':
                curves = curves * 0.00254 
                unit = 'm'
            else:
                unit = channel.units
        else:
            description = channel.long_name
            channel_name = channel.name
            unit = channel.units
        
        # Add the data to the LAS file
        las_file.append_curve(
            channel_name,
            curves,
            unit=unit,
            descr=description
        )
        
# No for acima, percorremos os channels do frame e verificamos se estão na lista de curvas desejadas.
# Renomeamos para DEPT a curva de profundidade (INDEX..) e, se necessário, convertimos de polegadas para metros.
# As demais curvas são extraídas normalmente e, após a verificação, todas são anexadas ao objeto LAS.


# Verificar se as informações da nossa curva foram transmitidas:

las_file.curves

# Salvar LAS
las_file.write('C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/DLIS_to_LAS_stage1/1MB_1_SC.las')
