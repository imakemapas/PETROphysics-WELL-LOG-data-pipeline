# -*- coding: utf-8 -*-
"""
Created on Sat Apr 12 17:31:44 2025

@author: Theresa Rocco Pereira Barbosa
"""

import os
import lasio
from tqdm import tqdm
from collections import defaultdict

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

BASE_PATH     = "C:/Users/Theresa/OneDrive/PESQUISA/LabMeg_Exxon/PMP_BC_WELL_LOGGING/03_LAS2IntegratedDF/"
LAS_INPUT_DIR = os.path.join(BASE_PATH, "LAS_input")
OUTPUT_DIR    = os.path.join(BASE_PATH) #, "DF_output")

# =============================================================================
# DICIONÁRIO COMPLETO DE PADRONIZAÇÃO DE CANAIS
# =============================================================================
CHANNEL_MAPPING = {
    # Profundidade e identificação
    'DEPT': 'DEPTH',
    'MD': 'DEPTH',
    
    # Raios Gama
    'GR': 'GR',
    'GR_1': 'GR',
    'GR_2': 'GR',
    'GRT': 'GR',
    'RGSS': 'GR_SHALLOW',
    'SGR': 'GR_SHALLOW',
    'SGR_1': 'GR_SHALLOW',
    'CGR': 'GR_COMPENSATED',
    'CGR_1': 'GR_COMPENSATED',
    'RHGR': 'GR_HYDROGEN',
    'HGR': 'GR_HYDROGEN',
    'HGR_1': 'GR_HYDROGEN',
    
    # Potencial Espontâneo
    'SP': 'SP',
    'SP_1': 'SP',
    'SP_2': 'SP',
    
    # Resistividade
    'ILD': 'RES_DEEP',
    'ILD_1': 'RES_DEEP',
    'ILD_2': 'RES_DEEP',
    'RILD': 'RES_DEEP',
    'ILT': 'RES_DEEP',
    'RLN': 'RES_MEDIUM',
    'RSN': 'RES_SHALLOW',
    'RLL': 'RES_LATEROLOG',
    'RLL_1': 'RES_LATEROLOG',
    'LLD': 'RES_LATEROLOG_DEEP',
    'LLD_1': 'RES_LATEROLOG_DEEP',
    'LLS': 'RES_LATEROLOG_SHALLOW',
    'LLS_1': 'RES_LATEROLOG_SHALLOW',
    'MSFL': 'RES_MICROSPHERICAL',
    'MSFL_1': 'RES_MICROSPHERICAL',
    'SFLU': 'RES_UNFOCUSED_SHALLOW',
    'SFLU_1': 'RES_UNFOCUSED_SHALLOW',
    'SFLU_2': 'RES_UNFOCUSED_SHALLOW',
    'SFLA': 'RES_FOCUSED_SHALLOW',
    'SFLA_1': 'RES_FOCUSED_SHALLOW',
    'SFLA_2': 'RES_FOCUSED_SHALLOW',
    'SFLT': 'RES_SHALLOW',
    
    # Densidade
    'RHOB': 'DEN',
    'RHOB_1': 'DEN',
    'RHOB_2': 'DEN',
    'RHOT': 'DEN',
    'DRHO': 'DEN_CORR',
    'DRHO_1': 'DEN_CORR',
    'DRHO_2': 'DEN_CORR',
    
    # Porosidade Neutrônica
    'NPHI': 'NEUT',
    'NPHI_1': 'NEUT',
    'NPHI_2': 'NEUT',
    'NEUT': 'NEUT',
    'TNPH': 'NEUT',
    'TNPH_1': 'NEUT',
    'NPOR': 'NEUT',
    'NPOR_1': 'NEUT',
    
    # Sônico
    'DT': 'SONIC',
    'DT_1': 'SONIC',
    'DT_2': 'SONIC',
    'DTSI': 'SLOWNESS',
    'DTSI_1': 'SLOWNESS',
    'DTSI_2': 'SLOWNESS',
    'TTI': 'SONIC_INT',
    'TTI_1': 'SONIC_INT',
    'TTI_2': 'SONIC_INT',
    'TTI_3': 'SONIC_INT',
    'TTI_4': 'SONIC_INT',
    'TTID': 'SONIC_INT',
    
    # Caliper
    'CALI': 'CALI',
    'CALI_1': 'CALI',
    'CALI_2': 'CALI',
    'CALI_3': 'CALI',
    'CALI_4': 'CALI',
    'CAL': 'CALI',
    'CIL': 'CALI',
    'CALS': 'CALI',
    'CAL1': 'CALI',
    'CALT': 'CALI',
    
    # Imageamentos e direcionais
    'DEVI': 'DEVIATION',
    'DEVI_1': 'DEVIATION',
    'AZIM': 'AZIMUTH',
    'HAZI': 'AZIMUTH',
    'AZIE': 'AZIMUTH',
    'DIP': 'DIP',
    'P1AZ': 'AZIMUTH_P1',
    'RB': 'R_BULK',
    'RAD1': 'RADIAL_1',
    'RAD2': 'RADIAL_2',
    'RAD3': 'RADIAL_3',
    'RAD4': 'RADIAL_4',
    
    # Pressão e tensão
    'PESS': 'PRESSURE',
    'TENS': 'TENSION',
    'TENS_1': 'TENSION',
    'TENS_2': 'TENSION',
    'TENS_3': 'TENSION',
    
    # Geoquímica/Espectrometria
    'POTA': 'POTASSIUM',
    'POTA_1': 'POTASSIUM',
    'THOR': 'THORIUM',
    'THOR_1': 'THORIUM',
    'URAN': 'URANIUM',
    'URAN_1': 'URANIUM',
    'W1NG': 'WINDOW_1',
    'W1NG_1': 'WINDOW_1',
    'W2NG': 'WINDOW_2',
    'W2NG_1': 'WINDOW_2',
    'W3NG': 'WINDOW_3',
    'W3NG_1': 'WINDOW_3',
    'W4NG': 'WINDOW_4',
    'W4NG_1': 'WINDOW_4',
    'W5NG': 'WINDOW_5',
    'W5NG_1': 'WINDOW_5',
    
    # Outros (agrupando por similaridade)
    'ITT': 'TEMP_INTAKE',
    'ITT_1': 'TEMP_INTAKE',
    'ITT_2': 'TEMP_INTAKE',
    'TOT': 'TEMP_OUT',
    'TOT_1': 'TEMP_OUT',
    'TOT_2': 'TEMP_OUT',
    'TOT_3': 'TEMP_OUT',
    'TOT_4': 'TEMP_OUT',
    'TOTD': 'TEMP_OUT',
    'SN': 'NEAR_SONIC',
    'SN_1': 'NEAR_SONIC',
    'SN_2': 'NEAR_SONIC',
    'CILD': 'CALI_DUAL',
    'CILD_1': 'CALI_DUAL',
    'CILD_2': 'CALI_DUAL',
    'ILM': 'RES_MEDIUM',
    'ILM_1': 'RES_MEDIUM',
    'ILM_2': 'RES_MEDIUM',
    'LL3': 'RES_LATEROLOG_3',
    'LL3_1': 'RES_LATEROLOG_3',
    'FCNL': 'FOCUSED_CURRENT',
    'FCNL_1': 'FOCUSED_CURRENT',
    'FCNL_2': 'FOCUSED_CURRENT',
    'NCNL': 'NEAR_CURRENT',
    'NCNL_1': 'NEAR_CURRENT',
    'NCNL_2': 'NEAR_CURRENT',
    'AMP': 'AMPLITUDE',
    'AMP_1': 'AMPLITUDE',
    'AMP_2': 'AMPLITUDE',
    'SRAT': 'S_RATIO',
    'SRAT_1': 'S_RATIO',
    'NRAT': 'N_RATIO',
    'NRAT_1': 'N_RATIO',
    'PEF': 'PHOTO_EFFECT',
    'PEF_1': 'PHOTO_EFFECT',
    'HPEF': 'PHOTO_EFFECT_H',
    'HPEF_1': 'PHOTO_EFFECT_H',
    
    # Padronização de sufixos numéricos (_1, _2, etc.)
    **{f'{base}_{n}': base for base in [
        'MINV', 'MNOR', 'MLL', 'C13', 'C24', 'LITH', 'LL', 'LS', 'LSRH', 
        'LU', 'LURH', 'SS1', 'SS2', 'SS1RH', 'DB1', 'DB2', 'DB3', 'DB4',
        'SB1', 'SB2', 'PART', 'CLEX', 'SHRP', 'NP', 'HT12', 'HT23', 'HT34',
        'HT41', 'CDDS', 'DCX', 'NM13', 'NM24', 'OFLG', 'KFLG', 'TIME', 'RGR',
        'RWA', 'CS', 'HSTA', 'REFE', 'RC', 'EMEX', 'PP', 'TEMP', 'FEP1', 'FEP2',
        'RC1', 'RC2', 'FEP', 'CBL', 'FFDC', 'NFDC', 'TT1', 'TT2', 'TT3', 'TT4',
        'DPAP', 'CL', 'NSDR', 'VDEP', 'EWDR', 'RX0', 'RX', 'DDQR', 'CBFS', 'CBLF',
        'SMIN', 'SMNO', 'DTL', 'TSPD', 'T1', 'T2', 'T3', 'T4', 'CNST', 'NPHS',
        'NPHD', 'DPHI', 'PHI', 'P15V', 'M60V', 'P60V', 'VREF', 'SPE', 'FISH',
        'HSP', 'ZB', 'ZM', 'DZB', 'DZM', 'DQZE', 'DQCA', 'MQZE', 'MQCA', 'SPQZ',
        'SPQC', 'PSQ', 'DRCO', 'DXCO', 'CCER', 'MRCO', 'MXCO', 'VRES', 'XFRC',
        'HDRS', 'HMRS', 'DFL', 'XHDR', 'XHMR', 'XDFL', 'HDCN', 'HMCN', 'HDR',
        'HDX', 'HMR', 'HMX', 'STEM', 'GND', 'P5V', 'M15V', 'LAT', 'LN', 'QUAF',
        'S12', 'S23A', 'S34A', 'S4A1', 'S13A', 'DPL1', 'DPL2', 'DB1A', 'DB2A',
        'DB3A', 'DB4A', 'REV', 'GNOR', 'FNOR', 'FINC', 'EV', 'EI', 'ETIM', 'RLU',
        'RLS', 'RLIT', 'RSS1', 'RSS2', 'QLS', 'QSS', 'NRHO', 'TNRA', 'RCFT', 'RCNT',
        'RLUL', 'RLUU', 'RSLL', 'RSLU', 'RSUL', 'RSUU', 'PARI', 'LSHV', 'SSHV', 'FFSS',
        'FFLS', 'SLDT', 'QRLS', 'QRSS', 'DALP', 'RTNR', 'TALP', 'NUCA', 'ENRA', 'RCEF',
        'RCEN', 'ENPH', 'CFTC', 'CNTC', 'CFEC', 'CNEC', 'RCAL', 'SA1', 'SA2', 'SA3',
        'RHGX', 'BS', 'SHVD', 'LHVD', 'RLLL', 'RLLU', 'RHLL', 'RHLU', 'RHLS', 'RHLI',
        'RHS1', 'RHS2', 'HNRH', 'HDAL', 'HPHN', 'HRHO', 'HDRH', 'HDPH', 'HLL', 'HLU',
        'HLS', 'HLIT', 'HSS1', 'HSS2', 'RHFT', 'RHNT', 'HTNP', 'HNPO', 'HCFT', 'HCNT',
        'HTAL', 'IHV', 'ICV', 'HDIR', 'DIPA', 'GRAD', 'DRIF', 'DRAZ', 'RLLD', 'RLLS',
        'CILM', 'CMSF', 'XLL3', 'FREC', 'CNST_1', 'CNST_2', 'TSPD_1', 'TSPD_2', 'TSPD_3',
        'CILM_1', 'CILM_2', 'HDFL', 'AZAP', 'SPHI', 'STSG', 'CTEM', 'CCSW', 'HV', 'RSPA',
        'RSP', 'SPAR', 'SPMV', 'DIFF', 'MARK', 'IICS', 'RSFL', 'RILM', 'TOD', 'SSLT',
        'SVEL', 'RLUL_1', 'RLUU_1', 'RSLL_1', 'RSLU_1', 'RSUL_1', 'RSUU_1', 'PARI_1',
        'LSHV_1', 'SSHV_1', 'FFSS_1', 'FFLS_1', 'SLDT_1', 'QRLS_1', 'QRSS_1', 'DALP_1',
        'RTNR_1', 'TALP_1', 'NUCA_1', 'ENRA_1', 'RCEF_1', 'RCEN_1', 'ENPH_1', 'CFTC_1',
        'CNTC_1', 'CFEC_1', 'CNEC_1', 'RCAL_1', 'SA1_1', 'SA2_1', 'SA3_1', 'RHGX_1',
        'BS_1', 'SHVD_1', 'LHVD_1', 'RLLL_1', 'RLLU_1', 'RHLL_1', 'RHLU_1', 'RHLS_1',
        'RHLI_1', 'RHS1_1', 'RHS2_1', 'HPEF_1', 'HNRH_1', 'HDAL_1', 'HPHN_1', 'HRHO_1',
        'HDRH_1', 'HDPH_1', 'HLL_1', 'HLU_1', 'HLS_1', 'HLIT_1', 'HSS1_1', 'HSS2_1',
        'RHFT_1', 'RHNT_1', 'HTNP_1', 'HNPO_1', 'HCFT_1', 'HCNT_1', 'HTAL_1', 'RHGR_1',
        'HGR_1', 'IHV_1', 'ICV_1', 'ILM_2', 'SFLA_2', 'SPAR_1', 'SPAR_2', 'TIME_1',
        'TIME_2', 'TT1_2', 'CS_2', 'TT2_2', 'TT3_2', 'TT4_2', 'ETIM_2', 'HNPO_1',
        'HTNP_1', 'RHFT_1', 'RHNT_1', 'HDRH_1', 'HNRH_1', 'HRHO_1', 'RHLI_1', 'RHLL_1',
        'RHLS_1', 'RHLU_1', 'RHS1_1', 'RHS2_1', 'AMP_2', 'LLD_1', 'LLS_1', 'MSFL_1',
        'DTSI_1', 'DTSI_2', 'CALI_4', 'DDQR', 'CBFS', 'CBLF', 'SMIN', 'SMNO', 'DTL',
        'CAL1', 'TOT_3', 'TOT_4', 'TTI_3', 'TTI_4', 'SFLT'
    ] for n in range(1, 10)}
}

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def remove_duplicate_columns(las):
    """Remove colunas duplicadas mantendo a primeira ocorrência de cada canal padronizado"""
    seen = set()
    unique_curves = []
    
    for curve in las.curves:
        # Padroniza o nome do canal
        standardized = CHANNEL_MAPPING.get(curve.mnemonic, curve.mnemonic)
        
        # Verifica se já vimos este canal padronizado
        if standardized not in seen:
            seen.add(standardized)
            unique_curves.append(curve)
    
    # Atualiza as curvas no objeto LAS
    las.curves = unique_curves
    return las

def list_las_files(directory):
    """Lista todos os arquivos .las no diretório especificado"""
    return [f for f in os.listdir(directory) if f.lower().endswith('.las')]

def get_las_metadata(filepath):
    """Extrai metadados básicos de um arquivo LAS com tratamento robusto de erros"""
    try:
        las = lasio.read(filepath)
        
        # Verifica se o objeto LAS foi lido corretamente
        if not hasattr(las, 'curves'):
            print(f"⚠️ Estrutura inválida em {os.path.basename(filepath)} - ignorando arquivo")
            return None
            
        # Remove duplicatas
        las = remove_duplicate_columns(las)
        
        # Extrai metadados
        well_name = getattr(las.well, 'WELL', las.well.get('WELL', 'N/A'))
        if hasattr(well_name, 'value'):
            well_name = well_name.value
            
        start = getattr(las.well, 'STRT', las.well.get('STRT', 'N/A'))
        depth_units = start.unit if hasattr(start, 'unit') else 'N/A'
        
        standardized_channels = []
        for curve in las.curves:
            standardized = CHANNEL_MAPPING.get(curve.mnemonic, curve.mnemonic)
            standardized_channels.append(standardized)
        
        return {
            'filename': os.path.basename(filepath),
            'well_name': well_name,
            'channels': standardized_channels,
            'depth_range': (round(las.index[0], 2), round(las.index[-1], 2)),
            'depth_units': depth_units,
            'num_points': len(las.index)
        }
        
    except Exception as e:
        print(f"⚠️ Erro crítico ao processar {os.path.basename(filepath)}: {str(e)}")
        return None

def analyze_channels(metadata_list):
    """Analisa a distribuição de canais entre os arquivos LAS"""
    channel_distribution = defaultdict(int)
    well_info = []
    
    for meta in metadata_list:
        if meta is None:
            continue
        well_info.append({
            'well_name': meta['well_name'],
            'filename': meta['filename'],
            'num_channels': len(meta['channels']),
            'depth_range': meta['depth_range'],
            'depth_units': meta['depth_units']
        })
        for channel in meta['channels']:
            channel_distribution[channel] += 1
    
    return {
        'channel_distribution': dict(sorted(channel_distribution.items(), 
                                         key=lambda item: item[1], 
                                         reverse=True)),
        'well_info': well_info
    }

def save_report(well_info, channel_dist, success_count, error_count):
    """Salva relatório detalhado em arquivo"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, "las_analysis_report.txt")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        # Cabeçalho
        f.write("RELATÓRIO DE ANÁLISE DE ARQUIVOS LAS\n")
        f.write("="*60 + "\n\n")
        f.write(f"ARQUIVOS PROCESSADOS: {success_count + error_count}\n")
        f.write(f"• Sucesso: {success_count}\n")
        f.write(f"• Com erro: {error_count}\n\n")
        
        # Informações por poço
        f.write("INFORMAÇÕES POR POÇO:\n")
        f.write("-"*60 + "\n")
        for well in well_info:
            f.write(f"\n🔹 Poço: {well['well_name']} ({well['filename']})\n")
            f.write(f"• Número de canais: {well['num_channels']}\n")
            f.write(f"• Intervalo de profundidade: {well['depth_range'][0]} a {well['depth_range'][1]} {well['depth_units']}\n")
        
        # Distribuição de canais
        f.write("\n\nDISTRIBUIÇÃO DE CANAIS PADRONIZADOS (sem duplicatas):\n")
        f.write("-"*60 + "\n")
        f.write("LEGENDA DE PADRONIZAÇÃO:\n")
        for original, standardized in CHANNEL_MAPPING.items():
            f.write(f"{original} → {standardized}\n")
        
        f.write("\nFREQUÊNCIA DE CANAIS:\n")
        for channel, count in channel_dist.items():
            f.write(f"{channel}: {count} arquivos ({count/success_count:.1%})\n")
    
    print(f"\n✅ Relatório salvo em: {report_path}")

# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    # Verifica se diretório existe
    if not os.path.exists(LAS_INPUT_DIR):
        print(f"\n❌ Diretório não encontrado: {LAS_INPUT_DIR}")
        return
    
    # Lista todos os arquivos LAS
    las_files = list_las_files(LAS_INPUT_DIR)
    if not las_files:
        print("\n❌ Nenhum arquivo LAS encontrado no diretório especificado")
        return
    
    print(f"\n🔍 Encontrados {len(las_files)} arquivos LAS no diretório: {LAS_INPUT_DIR}")
    
    # Processa cada arquivo e coleta metadados
    metadata_list = []
    for las_file in tqdm(las_files, desc="📊 Processando arquivos LAS", unit="file"):
        filepath = os.path.join(LAS_INPUT_DIR, las_file)
        metadata = get_las_metadata(filepath)
        metadata_list.append(metadata)
    
    # Filtra metadados válidos
    valid_metadata = [m for m in metadata_list if m is not None]
    error_count = len(metadata_list) - len(valid_metadata)
    
    if not valid_metadata:
        print("\n❌ Nenhum arquivo LAS válido encontrado")
        return
    
    # Analisa distribuição de canais
    analysis_results = analyze_channels(valid_metadata)
    channel_dist = analysis_results['channel_distribution']
    well_info = analysis_results['well_info']
    
    # Exibe resultados
    print("\n📋 RESUMO GERAL")
    print(f"• Arquivos processados com sucesso: {len(valid_metadata)}")
    print(f"• Arquivos com problemas: {error_count}")
    print(f"• Canais únicos encontrados: {len(channel_dist)}")
    
    print("\n📊 DISTRIBUIÇÃO DE CANAIS PADRONIZADOS (top 10 mais comuns)")
    for i, (channel, count) in enumerate(list(channel_dist.items())[:10], 1):
        print(f"{i}. {channel}: {count} arquivos ({count/len(valid_metadata):.1%})")
    
    # Salva relatório detalhado
    save_report(well_info, channel_dist, len(valid_metadata), error_count)

if __name__ == "__main__":
    print("="*60)
    print("INICIANDO ANÁLISE DE ARQUIVOS LAS")
    print("="*60)
    
    main()