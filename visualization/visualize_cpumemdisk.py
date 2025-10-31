import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from file_io import save_plot


load_dotenv()

BROKER_COLORS = {
    'kafka': 'red',
    'memphis': 'purple',
    'rabbitmq': 'blue',
    'redpanda': 'orange'
}

LINE_STYLES = {
    '1-1-1-AT_LEAST_ONCE': '-',
    '1-1-1-AT_MOST_ONCE': '--',
    '1-1-1-EXACTLY_ONCE': ':',
    '1-3-3-AT_LEAST_ONCE': '--',
    '5-3-3-AT_LEAST_ONCE': ':',
    '2-2-2-AT_LEAST_ONCE': '--',
    '4-4-4-AT_LEAST_ONCE': ':',
    '8-8-8-AT_LEAST_ONCE': '-.'
}


def _get_metric_data(df, metric_type):
    """지표 유형에 따라 그래프에 사용할 데이터를 반환"""
    if df.empty:
        if metric_type == 'cpu':
            return pd.Series(dtype=float), 'Total CPU Usage (%)'
        elif metric_type == 'memory':
            return pd.Series(dtype=float), 'Total Used Memory (%)'
        elif metric_type == 'iowait':
            return pd.Series(dtype=float), 'I/O Wait (%)'
        elif metric_type == 'iops':
            return pd.Series(dtype=float), 'Write IOps (Operations per second)'
        elif metric_type == 'throughput':
            return pd.Series(dtype=float), 'Write Throughput (MiB/s)'
        return pd.Series(), ''

    if metric_type == 'cpu':
        # CPU 사용량: Busy User, System, Iowait, Other의 합산
        return df['Busy User'] + df['Busy System'] + df['Busy Iowait'] + df['Busy Other'], 'Total CPU Usage (%)'
    elif metric_type == 'memory':
        # 메모리 사용량: Used와 Cache + Buffer의 합산
        return df['Used'] + df['Cache + Buffer'], 'Total Used Memory (%)'
    elif metric_type == 'iowait':
        return df['I/O Wait (%)'], 'I/O Wait (%)'
    elif metric_type == 'iops':
        # Write IOps만 반환
        return df['Write IOps'], 'Write IOps (Operations per second)'
    elif metric_type == 'throughput':
        # Disk Write Throughput만 반환
        MB_PER_BYTE = 1024 * 1024
        return df['Write Throughput (B/s)'] / MB_PER_BYTE, 'Write Throughput (MiB/s)'
    return pd.Series(), '' # 에러 방지


def _plot_resource_comparison(
    brokers, 
    group_name,
    metric_type, 
    comparison_group_info, 
    all_resource_data
):
    """
    지표에 대해 지정된 디렉토리들을 비교하는 그래프를 생성하고 저장
    """
    directories = comparison_group_info['directories']
    labels = comparison_group_info['labels']
    
    metric_title = metric_type.replace('_', ' ').title()
    if metric_type == 'iops':
        metric_title = 'Write IOps'
    elif metric_type == 'throughput':
        metric_title = 'Disk Write Throughput'


    # A. 단일 브로커 개별 그래프 (1-A)
    for broker in brokers:
        fig, ax = plt.subplots(figsize=(15, 6))
        ax.set_title(f'Comparison of {metric_title} - {broker} ({group_name})', fontsize=14)
        ax.set_xlabel('Time (minute)', fontsize=12)

        
        _, y_label = _get_metric_data(pd.DataFrame(), metric_type)
        ax.set_ylabel(y_label, fontsize=12)
        
        # 비교 그룹(디렉토리)별로 라인을 그립니다.
        for directory in directories:
            if directory not in all_resource_data or broker not in all_resource_data[directory]: continue

            # 해당 디렉토리와 브로커의 데이터프레임
            df = all_resource_data[directory][broker].get(metric_type, pd.DataFrame())
            if df.empty: continue
            
            # _get_metric_data 함수를 통해 Series를 얻음
            plot_data, _ = _get_metric_data(df, metric_type)
            
            if plot_data.empty: continue
            
            # 스타일 및 라벨 정의
            linestyle = LINE_STYLES.get(directory, '-') 
            legend_label = labels.get(directory, directory) # 'At Least Once'

            color = BROKER_COLORS.get(broker, 'gray') # A 섹션에서는 브로커 색상을 고정

            # 단일 지표 처리
            ax.plot(plot_data.index, plot_data, 
                    label=legend_label,
                    color=color, 
                    linestyle=linestyle)
                
        ax.set_ylim(bottom=0)
        ax.legend(loc='upper left', fontsize=9)
        ax.grid(True, axis='both', linestyle='--')
        fig.tight_layout()
        save_plot(fig, f'{group_name}/{metric_type}', f'{broker}') 
        plt.close(fig)

    # B. 전체 브로커 한 번에 표시 (1-B)
    # 단일 지표이므로, 하나의 Figure만 생성합니다.
    
    fig, ax = plt.subplots(figsize=(18, 8))
    ax.set_title(f'Comparison of {metric_title} Across All Brokers ({group_name})', fontsize=14)
    ax.set_xlabel('Time (minute)', fontsize=12)
    
    # Y축 레이블 설정 
    _, y_label = _get_metric_data(pd.DataFrame(), metric_type)
    ax.set_ylabel(y_label, fontsize=12)


    # 브로커별, 디렉토리별로 라인을 그립니다.
    for broker in brokers:
        for directory in directories:
            if directory not in all_resource_data or broker not in all_resource_data[directory]: continue

            df = all_resource_data[directory][broker].get(metric_type, pd.DataFrame())
            if df.empty: continue
                
            plot_data_full, _ = _get_metric_data(df, metric_type)
            
            if plot_data_full.empty: continue
            
            # 스타일 및 라벨 정의
            linestyle = LINE_STYLES.get(directory, '-')
            legend_label = labels.get(directory, directory) # 'At Least Once'
            color = BROKER_COLORS.get(broker, 'gray')
            
            # 최종 범례 라벨: 'kafka (At Least Once)'
            full_legend_label = f'{broker} ({legend_label})' 
            
            # 단일 지표이므로 plot_data_full을 그대로 사용
            ax.plot(plot_data_full.index, plot_data_full, 
                    label=full_legend_label,
                    color=color, 
                    linestyle=linestyle)

    ax.set_ylim(bottom=0)
    ax.legend(loc='upper left', bbox_to_anchor=(0, 1), fontsize=9, ncol=len(brokers)) # 범례를 보기 쉽게 조정
    ax.grid(True, axis='both', linestyle='--')
    fig.tight_layout()
    
    # 저장 파일명: 그룹_지표_all_brokers_comparison
    save_plot(fig, f'{group_name}/{metric_type}', f'all_brokers')
    plt.close(fig)


def draw_comparison_resource_graphs(brokers, all_resource_data, comparison_groups):
    """
    모든 비교 그룹 및 모든 리소스 지표에 대해 비교 그래프를 생성하는 메인 함수
    """
    
    resource_metrics = ['cpu', 'memory', 'iowait', 'iops', 'throughput']
    
    for group_name, info in comparison_groups.items():
        print(f"Drawing comparison graphs for group: {group_name}...", end=' ')
        for metric in resource_metrics:
            _plot_resource_comparison(brokers, group_name, metric, info, all_resource_data)
        print("done.")
