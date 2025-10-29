import os
import datetime
import subprocess
import re
import glob
import time
from dotenv import load_dotenv

# ----------------------------------------------------------------------
# --- 설정 (Configuration) ---
# ----------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PARAM_FILE_NAME = "param.conf"
MIN_METRICS_LINES = 100
MAX_FILE_AGE_MINUTES = 30 # 파일 생성 시각 최대 허용 시간 (30분)

# 테스트를 위한 시간 상수 (분 단위)
UP_WAIT_TIME_MINUTES = 0.2
DOWN_WAIT_TIME_MINUTES = 0.2
MAX_CHECK_DURATION_MINUTES = 15
CHECK_INTERVAL_SECONDS = 5

# 서브태스크 디렉토리와 스크립트 정의
SUBTASKS_CONFIG = {
    "kafka": {"up": "up.sh", "down": "down.sh"},
    "memphis": {"up": "up.sh", "down": "down.sh"},
    "rabbitmq": {"up": "up.sh", "down": "down.sh"},
    "redpanda": {"up": "up.sh", "down": "down.sh"},
}
VISUALIZATION_TASK = "visualization"
VISUALIZATION_SCRIPT = "visualize.sh"

# .env 파일 로드
load_dotenv()
try:
    BENCHMARK_WARMUP_MINUTES = float(os.getenv("BENCHMARK_WARMUP_MINUTES", 0))
    BENCHMARK_DURATION_MINUTES = float(os.getenv("BENCHMARK_DURATION_MINUTES", 0))
except ValueError:
    print("ERROR: BENCHMARK_WARMUP_MINUTES or BENCHMARK_DURATION_MINUTES in .env is not a valid float.")
    BENCHMARK_WARMUP_MINUTES = 0.0
    BENCHMARK_DURATION_MINUTES = 0.0

# ----------------------------------------------------------------------
# --- 헬퍼 함수 (Helper Functions) ---
# ----------------------------------------------------------------------

def _log_message(parameter: str, level: str, message: str):
    """
    로그를 작성하고 콘솔에 출력하며, WARN/ERROR 레벨은 .error.log에 추가 기록합니다.
    DEBUG 레벨은 일반 로그 파일에만 기록하고 콘솔에는 출력하지 않습니다.
    """
    timestamp = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
    date_prefix = datetime.datetime.now().strftime("%Y%m%d")
    
    # 1. 일반 로그 파일 (모든 레벨 기록)
    log_filename = date_prefix + ".log"
    log_filepath = os.path.join(PROJECT_ROOT, log_filename)
    
    log_line = f"{timestamp} [{level}] {parameter} {message}\n"
    
    with open(log_filepath, "a", encoding="utf-8") as f:
        f.write(log_line)
    
    # 2. 에러 로그 파일 (WARN/ERROR 레벨만 추가 기록)
    if level in ["WARN", "ERROR"]:
        error_log_filename = date_prefix + ".error.log"
        error_log_filepath = os.path.join(PROJECT_ROOT, error_log_filename)
        
        with open(error_log_filepath, "a", encoding="utf-8") as f:
            f.write(log_line)
            
    # 3. 콘솔 출력 (DEBUG 레벨은 출력하지 않음. WARN/ERROR 포함)
    if level != "DEBUG":
        print(log_line.strip(), flush=True)

def _write_result_log(parameter: str, final_success: bool, subtask_results: dict[str, str], failure_reason: str, is_final: bool = False):
    """
    yyyyyymmdd.result.log 파일에 최종 또는 중간 결과를 기록합니다.
    [TIME] [SUBTASK_RESULT] <PARAMETER> <서브태스크명>: <상태> (<Progress|Final>) 형식을 따릅니다.
    """
    timestamp = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
    date_prefix = datetime.datetime.now().strftime("%Y%m%d")
    
    log_filepath = os.path.join(PROJECT_ROOT, date_prefix + ".result.log")
    
    log_lines = []
    
    # 1. 서브태스크/태스크 결과 라인 생성
    if is_final:
        # 최종 결과 기록: 전체 요약 + 개별 서브태스크 결과 모두 기록
        
        # A. 태스크 요약
        overall_status = "SUCCESS" if final_success else "FAILURE"
        # [TIME] [TASK_STATUS] <PARAMETER> Overall: <STATUS>
        log_lines.append(f"{timestamp} [TASK_STATUS] {parameter} Overall: {overall_status}")

        if not final_success and failure_reason:
            # [TIME] [TASK_REASON] <PARAMETER> Reason: <REASON>
            log_lines.append(f"{timestamp} [TASK_REASON] {parameter} Reason: {failure_reason}")
            
        # B. 개별 서브태스크 결과
        for name, status in subtask_results.items():
            # [TIME] [SUBTASK_RESULT] <PARAMETER> <서브태스크명>: <상태>
            log_lines.append(f"{timestamp} [SUBTASK_RESULT] {parameter} {name.capitalize()}: {status}")

        log_lines.append("-" * 50)
    else:
        # 중간 서브태스크 결과 기록: 요청된 형식에 맞춰 (Progress) 토큰과 함께 기록
        # subtask_results는 {서브태스크명: 상태} 형태여야 함
        for name, status in subtask_results.items():
            # [TIME] [SUBTASK_RESULT] <PARAMETER> <서브태스크명>: <상태> (Progress)
            log_lines.append(f"{timestamp} [SUBTASK_RESULT] {parameter} {name.capitalize()}: {status} (Progress)")

    try:
        with open(log_filepath, "a", encoding="utf-8") as f:
            f.write('\n'.join(log_lines) + '\n')
    except Exception as e:
        _log_message(parameter, "ERROR", f"Failed to write to result log file: {e}")


def _load_parameters_from_file(filepath: str) -> list[str]:
    """param.conf 파일로부터 파라미터 목록을 읽어옵니다."""
    parameters = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line.startswith("#"):
                    continue
                if stripped_line:
                    parameters.append(stripped_line)
        return parameters
    except FileNotFoundError:
        print(f"ERROR: Parameter file not found at {filepath}")
        return []
    except Exception as e:
        print(f"ERROR: Failed to read parameter file: {e}")
        return []

def _parse_parameter(parameter: str) -> tuple[int, int, str]:
    """파라미터 문자열을 파싱하여 producer_count, consumer_count, 디렉토리 이름을 반환합니다."""
    # 수정: 구분자를 공백( )으로 변경
    parts = parameter.split(' ', 3)
    if len(parts) < 4:
        raise ValueError(f"Invalid parameter format: {parameter}")
    
    try:
        producer_count = int(parts[0])
        consumer_count = int(parts[2])
    except ValueError:
        raise ValueError(f"Producer/Consumer count must be integers in: {parameter}")

    # 변경된 부분: 디렉토리 이름은 전체 파라미터를 하이픈으로 연결하여 생성
    dir_name = '-'.join(parameter.split(' ')) 

    return producer_count, consumer_count, dir_name

def _execute_shell_script(script_path: str, parameter: str) -> bool:
    """
    쉘 스크립트를 실행합니다.
    (성공 판정: return code 0 이며, stdout에 'docker compose (up|down) done' 또는 'visualization done'이 등장한 후의 stderr에 'warning' 토큰이 없는 라인이 없어야 성공)
    """
    script_dir = os.path.dirname(script_path)
    
    if not os.path.exists(script_path):
        _log_message(parameter, "ERROR", f"Shell script file not found: {script_path}")
        return False
        
    if not os.access(script_path, os.X_OK):
        _log_message(parameter, "ERROR", f"Shell script is not executable (missing +x permission): {script_path}")
        return False

    _log_message(parameter, "INFO", f"Executing shell script: {script_path} in dir: {script_dir} with parameter: {parameter}")
    
    try:
        # NOTE: parameter가 공백으로 구분되어 있으므로, command를 실행할 때 쉘이 이를 여러 개의 인자로 분리함
        command = f"/bin/bash {script_path} {parameter}"
        
        result = subprocess.run(
            command,
            cwd=script_dir,
            shell=True,
            check=False,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        # 1. stdout/stderr 분석을 위한 준비
        stdout_output = result.stdout.strip()
        stderr_output = result.stderr.strip()
        
        # DEBUG 로그 추가: stdout/stderr 전체 내용 기록
        if stdout_output:
            _log_message(parameter, "DEBUG", f"Script stdout (full):\n{stdout_output}")
        if stderr_output:
            _log_message(parameter, "DEBUG", f"Script stderr (full):\n{stderr_output}")

        has_critical_error = False
        critical_error_line = ""

        # 2. 'done' 토큰 조건 확인
        done_condition_met = False
        stdout_lower = stdout_output.lower()
        
        # 스크립트 이름에 따라 'done' 토큰을 다르게 적용
        is_visualization_script = os.path.basename(script_path) == VISUALIZATION_SCRIPT
        
        if is_visualization_script:
            if "visualization done" in stdout_lower:
                done_condition_met = True
                expected_done_token = "visualization done"
            else:
                expected_done_token = "visualization done"
        else:
            if "docker compose up done" in stdout_lower or "docker compose down done" in stdout_lower:
                done_condition_met = True
                expected_done_token = "docker compose (up|down) done"
            else:
                expected_done_token = "docker compose (up|down) done"

        if not done_condition_met:
            # 'done' 조건이 없으면 무조건 실패
            _log_message(parameter, "ERROR", f"Script execution FAILED: Required '{expected_done_token}' token not found in stdout.")
            return False

        # 3. 'done' 이후의 stderr가 있는지 확인
        if stderr_output:
            # 'done'이 나온 경우, 남아있는 stderr는 "done 이후"로 간주하고 검사
            
            stderr_lines = stderr_output.split('\n')
            
            # 무시할 키워드 목록 (Docker Compose의 정상적인 경고/상태 메시지)
            IGNORE_KEYWORDS = [
                'level=warning', 'waiting', 'exited', 'healthy', 'container',
                'built', 'stopping', 'stopped', 'removing', 'removed',
                'creating', 'created', 'starting', 'started', 
                'network', 'volume' 
            ]
            
            for line in stderr_lines:
                line_strip = line.strip()
                line_lower = line_strip.lower()

                # IGNORE_KEYWORDS 목록 중 하나라도 포함하는 경우 무시
                is_ignored = any(keyword in line_lower for keyword in IGNORE_KEYWORDS)
                
                # 무시되지 않고, 내용이 존재하는 stderr 라인이 발견되면 치명적인 에러로 간주
                if not is_ignored and line_strip != "":
                    has_critical_error = True
                    critical_error_line = line_strip
                    break
        
        # 4. 최종 성공/실패 판정
        if result.returncode == 0 and not has_critical_error:
            # 리턴 코드가 0이고, 'done'이 있으며, 치명적인 stderr가 없어야 성공
            _log_message(parameter, "INFO", f"Script execution SUCCESS: {script_path}. (Return code 0, '{expected_done_token}' found, and no critical stderr.)")
            return True
        else:
            # 리턴 코드가 0이 아니거나 치명적인 stderr가 있을 경우 실패
            failure_reason = ""
            if result.returncode != 0:
                failure_reason += f"Return Code {result.returncode}. "
            if has_critical_error:
                failure_reason += "Critical stderr detected. "
                _log_message(parameter, "ERROR", f"Script execution FAILED (Critical stderr detected after '{expected_done_token}' token): {critical_error_line}")
                
            # 'done' 미발견 실패는 이미 2단계에서 처리되었음

            _log_message(parameter, "ERROR", 
                         f"Script execution FAILED: {script_path}. "
                         f"Reason: {failure_reason.strip()}. Stderr (full): {'Logged in DEBUG' if stderr_output and not has_critical_error else 'Logged above'}")
            return False
            
    except subprocess.TimeoutExpired:
        _log_message(parameter, "ERROR", f"Script execution TIMEOUT after 300 seconds: {script_path}")
        return False
    except Exception as e:
        _log_message(parameter, "ERROR", f"An unexpected error occurred during script execution of {script_path}: {e}")
        return False

# ----------------------------------------------------------------------
# --- 성공 조건 검증 함수 (Success Criteria Validation) ---
# ----------------------------------------------------------------------

def _check_file_age(filepath: str, parameter: str) -> bool:
    """
    파일 이름의 타임스탬프를 파싱하여, 현재 GMT+9 시각 대비 30분 이내 차이가 나는지 검증합니다.
    (데이터 서브태스크: GMT+0 기준 / Visualization: GMT+9 기준)
    """
    
    filename = os.path.basename(filepath)
    parts = filename.split('-')
    
    if len(parts) < 2:
        _log_message(parameter, "ERROR", 
                     f"File age check FAILED: {filepath}. Could not find '-' separated timestamp token in filename.")
        return False
        
    # 파일 경로를 기반으로 visualization 서브태스크인지 확인
    is_visualization_file = VISUALIZATION_TASK in filepath.split(os.path.sep)
    
    # 타임스탬프 토큰 추출 로직
    if is_visualization_file:
        # Visualization: <이름>-<일시>.<확장자> -> 마지막 토큰이 <일시>.<확장자>
        time_token = parts[-1] 
        target_tz_offset = 9  # GMT+9
        tz_name = "GMT+9"
    else:
        # 데이터 서브태스크: <이름>-<일시>-<ID>.<확장자> -> 뒤에서 두 번째 토큰이 <일시>
        if len(parts) < 3:
             _log_message(parameter, "ERROR", 
                         f"File age check FAILED: {filepath}. Data file expected 3 tokens (name-time-id), found {len(parts)}.")
             return False
        time_token = parts[-2]
        target_tz_offset = 0  # GMT+0 (UTC)
        tz_name = "GMT+0 (UTC)"
    
    # 토큰에서 확장자를 제거 (예: 1029_095418.png -> 1029_095418)
    gmt_time_str_with_ext = time_token.split('.')[0] 

    # 정규식 패턴: MMDD_HHMMSS 형식 (4자리_6자리)
    match = re.fullmatch(r"(\d{4}_\d{6})", gmt_time_str_with_ext)
    
    if not match:
         _log_message(parameter, "ERROR", 
                     f"File age check FAILED: {filepath}. Extracted token '{gmt_time_str_with_ext}' does not match MMDD_HHMMSS format.")
         return False
         
    gmt_time_str = match.group(1)
    
    # 현재 연도(YY)를 추가하여 'YYMMDD_HHMMSS' 포맷 생성
    current_year = datetime.datetime.now().year % 100 
    full_time_str = f"{current_year}{gmt_time_str}" 
    
    try:
        # 1. 파싱된 문자열을 naive datetime 객체로 변환
        parsed_time_naive = datetime.datetime.strptime(full_time_str, '%y%m%d_%H%M%S')
        
        # 2. 파일 타임스탬프에 해당 시간대 정보 부여 (GMT+Offset)
        parsed_tz = datetime.timezone(datetime.timedelta(hours=target_tz_offset))
        parsed_time_aware = parsed_time_naive.replace(tzinfo=parsed_tz)
        
        # 3. 현재 GMT+9 시간 구하기
        kst_tz = datetime.timezone(datetime.timedelta(hours=9))
        current_time_kst = datetime.datetime.now(kst_tz)
        
        # 4. 비교를 위해 파싱된 시간을 GMT+9로 변환
        parsed_time_kst = parsed_time_aware.astimezone(kst_tz)
        
        # 5. 시간 차이 (절대값) 계산
        time_difference = abs(current_time_kst - parsed_time_kst)
        max_age_timedelta = datetime.timedelta(minutes=MAX_FILE_AGE_MINUTES) # 30분
        
        if time_difference <= max_age_timedelta:
            # 성공 시 로그는 _check_..._success_criteria에서 INFO 레벨로 출력
            return True
        else:
            # 실패 시 ERROR 로그 출력 (콘솔 포함)
            _log_message(parameter, "ERROR", 
                         f"File age check FAILED: {filepath}. "
                         f"Parsed Time ({tz_name}): {parsed_time_aware.strftime('%Y/%m/%d %H:%M:%S')}. "
                         f"Compared Time (GMT+9): {parsed_time_kst.strftime('%Y/%m/%d %H:%M:%S')}. "
                         f"Current Time (GMT+9): {current_time_kst.strftime('%Y/%m/%d %H:%M:%S')}. "
                         f"Difference: {time_difference}. Must be <= {MAX_FILE_AGE_MINUTES} min.")
            return False
            
    except ValueError as e:
        _log_message(parameter, "ERROR", 
                     f"File age check FAILED: {filepath}. Timestamp format error: {e}")
        return False
    except Exception as e:
        _log_message(parameter, "ERROR", f"Error checking file age for {filepath}: {e}")
        return False


def _check_file_content(filepath: str, expected_type: str, parameter: str) -> bool:
    """CSV 라인 수 또는 TXT 파일 형식을 검증합니다."""
    # 파일 시각 검증 로직을 _check_file_age로 대체
    if not _check_file_age(filepath, parameter):
        return False
        
    try:
        if expected_type == 'csv':
            with open(filepath, 'r', encoding='utf-8') as f:
                line_count = len(f.readlines()) - 1
            
            if line_count >= MIN_METRICS_LINES:
                return True
            else:
                _log_message(parameter, "ERROR", f"CSV content check failed: {filepath}. Expected >= {MIN_METRICS_LINES} data lines, got {line_count}.")
                return False

        elif expected_type == 'txt':
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            
            match = re.fullmatch(r"(\d+\.\d+),(\d+\.\d+)", content)
            if match:
                start_ts = float(match.group(1))
                end_ts = float(match.group(2))
                
                start_dt = datetime.datetime.fromtimestamp(start_ts)
                end_dt = datetime.datetime.fromtimestamp(end_ts)
                
                _log_message(parameter, "INFO", 
                             f"TXT content check passed: {filepath}. "
                             f"Start: {start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}, "
                             f"End: {end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}")
                return True
            else:
                _log_message(parameter, "ERROR", f"TXT content check failed: {filepath}. Content format invalid: {content}")
                return False
        
        return False
        
    except Exception as e:
        _log_message(parameter, "ERROR", f"Error checking file {filepath}: {e}")
        return False

def _check_data_subtask_success_criteria(subtask_name: str, parameter: str) -> bool:
    """kafka, rabbitmq, memphis, redpanda 서브태스크의 **모든** 성공 조건을 검증합니다."""
    try:
        producer_count, consumer_count, dir_name = _parse_parameter(parameter)
    except ValueError:
        _log_message(parameter, "WARN", f"Validation FAILED: parameter parsing failed: {parameter}")
        return False

    param_dir = os.path.join(PROJECT_ROOT, subtask_name, 'results', dir_name)
    
    # 1. 디렉토리 구조 검증
    if not os.path.isdir(param_dir):
        _log_message(parameter, "WARN", f"Validation FAILED: Required directory not found: {param_dir}.")
        return False

    producer_dir = os.path.join(param_dir, 'producer')
    consumer_dir = os.path.join(param_dir, 'consumer')

    if not os.path.isdir(producer_dir) or not os.path.isdir(consumer_dir):
        _log_message(parameter, "WARN", f"Validation FAILED: 'producer' or 'consumer' subdirectories not found in: {param_dir}.")
        return False
        
    # 2. 파일 개수 검증
    all_producer_metrics = glob.glob(os.path.join(producer_dir, 'producer_metrics-*-*.csv'))
    all_benchmark_times = glob.glob(os.path.join(producer_dir, 'benchmark_time-*-*.txt'))
    all_consumer_metrics = glob.glob(os.path.join(consumer_dir, 'consumer_metrics-*-*.csv'))

    if len(all_producer_metrics) != producer_count or \
       len(all_benchmark_times) != producer_count or \
       len(all_consumer_metrics) != consumer_count:
        
        _log_message(parameter, "WARN", 
                     f"Validation FAILED: File count mismatch. Prod Metrics: {len(all_producer_metrics)}/{producer_count}, "
                     f"Time: {len(all_benchmark_times)}/{producer_count}, "
                     f"Cons Metrics: {len(all_consumer_metrics)}/{consumer_count}.")
        return False

    # 3. 파일 내용 및 생성 시각 검증
    for metrics_file in all_producer_metrics:
        if not _check_file_content(metrics_file, 'csv', parameter): 
            _log_message(parameter, "WARN", f"Validation FAILED: Producer metrics content/age check failed for file: {metrics_file}. See ERROR log above.") 
            return False
            
    for time_file in all_benchmark_times:
        if not _check_file_content(time_file, 'txt', parameter): 
            _log_message(parameter, "WARN", f"Validation FAILED: Benchmark time content/age check failed for file: {time_file}. See ERROR log above.")
            return False

    for metrics_file in all_consumer_metrics:
        if not _check_file_content(metrics_file, 'csv', parameter): 
            _log_message(parameter, "WARN", f"Validation FAILED: Consumer metrics content/age check failed for file: {metrics_file}. See ERROR log above.")
            return False

    _log_message(parameter, "INFO", f"Data Subtask {subtask_name} file validation SUCCESS.")
    return True

def _check_visualization_subtask_success_criteria(parameter: str) -> bool:
    """visualization 서브태스크의 성공 조건을 검증합니다. (PNG 파일 개수 및 Age)"""
    try:
        _, _, dir_name = _parse_parameter(parameter)
    except ValueError:
        _log_message(parameter, "WARN", f"Visualization Validation FAILED: parameter parsing failed: {parameter}")
        return False

    param_dir = os.path.join(PROJECT_ROOT, VISUALIZATION_TASK, 'results', dir_name)
    
    if not os.path.isdir(param_dir):
        _log_message(parameter, "WARN", f"Visualization Validation FAILED: Directory not found: {param_dir}.")
        return False

    png_files = glob.glob(os.path.join(param_dir, '*.png'))
    
    # 1. 파일 개수 검증
    if len(png_files) == 0:
        _log_message(parameter, "WARN", f"Visualization Validation FAILED: Expected >0 PNG files, Found: {len(png_files)} in {param_dir}.")
        return False
    
    # 2. 파일 생성 시각 검증
    all_files_valid = True
    for png_file in png_files:
        if not _check_file_age(png_file, parameter):
            all_files_valid = False
            
    if all_files_valid:
        _log_message(parameter, "INFO", f"Visualization file check passed. Found >0 valid PNG files in {param_dir}")
        return True
    else:
        _log_message(parameter, "WARN", f"Visualization Validation FAILED: One or more PNG files failed the age check.")
        return False

# ----------------------------------------------------------------------
# --- 메인 태스크 함수 (Main Task Functions) ---
# ----------------------------------------------------------------------

def run_subtask(name: str, config: dict, parameter: str, task_results: dict[str, str]) -> bool:
    """
    개별 서브태스크를 시간 로직에 맞춰 up.sh, down.sh 순서로 실행하고 반복 검증합니다.
    """
    global BENCHMARK_WARMUP_MINUTES, BENCHMARK_DURATION_MINUTES
    
    _log_message(parameter, "INFO", f"--- Starting Subtask: {name} ---")
    
    up_script_path = os.path.join(PROJECT_ROOT, name, config['up'])
    down_script_path = os.path.join(PROJECT_ROOT, name, config['down'])

    # 1. UP 스크립트 실행
    if not _execute_shell_script(up_script_path, parameter):
        _log_message(parameter, "ERROR", f"Subtask {name} UP script failed. Subtask FAILURE.")
        task_results[name] = "FAILURE"
        _write_result_log(parameter, False, {name: "FAILURE"}, "")
        return False

    # 2. Init + Warmup + Benchmark 시간 대기
    pre_check_wait_minutes = UP_WAIT_TIME_MINUTES + BENCHMARK_WARMUP_MINUTES + BENCHMARK_DURATION_MINUTES
    pre_check_wait_seconds = pre_check_wait_minutes * 60
    
    _log_message(parameter, "INFO", 
                 f"Waiting for benchmark data generation: {pre_check_wait_minutes:.1f} min.")
    
    time.sleep(pre_check_wait_seconds)
    _log_message(parameter, "INFO", "Benchmark data generation time complete. Starting result verification window.")

    # 3. 3분 동안 10초마다 결과 확인 (성공 확정 시 중단)
    max_checks = int((MAX_CHECK_DURATION_MINUTES * 60) / CHECK_INTERVAL_SECONDS)
    validation_success = False
    
    for check_num in range(1, max_checks + 1):
        _log_message(parameter, "INFO", f"Verification attempt {check_num}/{max_checks}...")
        
        if _check_data_subtask_success_criteria(name, parameter):
            validation_success = True
            _log_message(parameter, "INFO", f"Subtask {name} SUCCESS validation on attempt {check_num}.")
            break
        else:
            _log_message(parameter, "WARN", f"Verification failed on attempt {check_num}. Check detailed logs above for reason.") 
        
        if check_num < max_checks:
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    # 4. DOWN 스크립트 실행 (UP 성공 시도 후)
    _log_message(parameter, "INFO", f"Executing DOWN script for {name}...")
    if not _execute_shell_script(down_script_path, parameter):
        _log_message(parameter, "WARN", f"Subtask {name} DOWN script failed. Proceeding with validation result.") 

    # 5. Post 대기 시간동안 대기
    post_wait_seconds = DOWN_WAIT_TIME_MINUTES * 60
    _log_message(parameter, "INFO", f"Waiting for POST clean up: {DOWN_WAIT_TIME_MINUTES} min.")
    time.sleep(post_wait_seconds)

    # 6. 최종 서브태스크 성공/실패 결정 및 실시간 기록
    if validation_success:
        _log_message(parameter, "INFO", f"--- Subtask {name} SUCCESS ---")
        task_results[name] = "SUCCESS"
        _write_result_log(parameter, True, {name: "SUCCESS"}, "")
        return True
    else:
        _log_message(parameter, "ERROR", f"--- Subtask {name} FAILURE (Validation Timeout) ---")
        task_results[name] = "FAILURE"
        _write_result_log(parameter, False, {name: "FAILURE"}, "")
        return False

def run_visualization_task(parameter: str, task_results: dict[str, str]) -> bool:
    """
    visualization/visualize.sh 스크립트를 실행하고 파일 검증을 수행합니다.
    """
    visualization_dir = os.path.join(PROJECT_ROOT, VISUALIZATION_TASK)
    script_path = os.path.join(visualization_dir, VISUALIZATION_SCRIPT)
    
    name = VISUALIZATION_TASK
    _log_message(parameter, "INFO", f"--- Starting Visualization Task ---")
    
    # 1. 스크립트 실행
    script_success = _execute_shell_script(script_path, parameter)
    
    if not script_success:
        _log_message(parameter, "ERROR", f"Visualization script execution failed. Skipping validation.")
        task_results[name] = "FAILURE"
        _write_result_log(parameter, False, {name: "FAILURE"}, "")
        return False
        
    # 2. 파일 검증 시작
    max_checks = int((MAX_CHECK_DURATION_MINUTES * 60) / CHECK_INTERVAL_SECONDS)
    validation_success = False
    
    for check_num in range(1, max_checks + 1):
        _log_message(parameter, "INFO", f"Visualization Verification attempt {check_num}/{max_checks}...")
        
        if script_success and _check_visualization_subtask_success_criteria(parameter):
            validation_success = True
            _log_message(parameter, "INFO", f"Visualization SUCCESS validation on attempt {check_num}.")
            break
        else:
            _log_message(parameter, "WARN", f"Visualization verification failed on attempt {check_num}. Check detailed logs above for reason.")
        
        if check_num < max_checks:
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    # 3. Post 대기 시간동안 대기 
    post_wait_seconds = DOWN_WAIT_TIME_MINUTES * 60
    _log_message(parameter, "INFO", f"Waiting for POST clean up: {DOWN_WAIT_TIME_MINUTES} min.")
    time.sleep(post_wait_seconds)
            
    # 4. 최종 서브태스크 성공/실패 결정 및 실시간 기록
    if validation_success:
        _log_message(parameter, "INFO", "--- Visualization Task SUCCESS ---")
        task_results[name] = "SUCCESS"
        _write_result_log(parameter, True, {name: "SUCCESS"}, "")
        return True
    else:
        _log_message(parameter, "ERROR", "--- Visualization Task FAILURE (Validation Timeout) ---")
        task_results[name] = "FAILURE"
        _write_result_log(parameter, False, {name: "FAILURE"}, "")
        return False


def run_benchmark_task(parameter: str) -> bool:
    """
    주어진 파라미터로 전체 벤치마크 태스크를 실행하고, 결과를 yyyymmdd.result.log에 기록합니다.
    """
    
    _log_message(parameter, "INFO", f"================================================")
    _log_message(parameter, "INFO", f"Starting Benchmark Task with Parameter: {parameter}")
    _log_message(parameter, "INFO", f"Warmup: {BENCHMARK_WARMUP_MINUTES} min, Duration: {BENCHMARK_DURATION_MINUTES} min")
    _log_message(parameter, "INFO", f"================================================")

    # 모든 서브태스크의 실시간 상태를 저장하는 딕셔너리
    subtask_successes: dict[str, str] = {name: "NOT_STARTED" for name in SUBTASKS_CONFIG}
    subtask_successes[VISUALIZATION_TASK] = "NOT_STARTED"

    # 1. kafka, memphis, rabbitmq, redpanda 4개 서브태스크 시도 (실패와 상관없이 모두 실행)
    for name, config in SUBTASKS_CONFIG.items():
        # run_subtask 내에서 subtask_successes에 결과가 업데이트되고 실시간 로그 기록
        run_subtask(name, config, parameter, subtask_successes)
        # 쉘 스크립트 실행 실패 시 (return False)도 task_results에 반영됨

    # 4개 서브태스크의 결과를 종합
    is_exactly_once = parameter.split(' ')[-1].upper() == "EXACTLY_ONCE"
    
    # RabbitMQ 결과 처리 (조건부 무시)
    is_rabbitmq_failed = subtask_successes.get("rabbitmq") == "FAILURE"
    
    main_subtask_statuses = [
        subtask_successes.get(name) for name in SUBTASKS_CONFIG.keys()
    ]

    all_main_subtasks_succeeded = True
    for name in SUBTASKS_CONFIG.keys():
        status = subtask_successes.get(name)
        if status != "SUCCESS":
            # rabbitmq의 조건부 무시 로직
            if not (name == "rabbitmq" and is_exactly_once):
                all_main_subtasks_succeeded = False
                break
    
    final_task_success = False
    visualization_success = False

    # 2. 모두 성공하면 visualization 서브태스크 실행
    if all_main_subtasks_succeeded:
        _log_message(parameter, "INFO", "All main subtasks succeeded (RabbitMQ failure conditionally ignored). Proceeding to Visualization.")
        
        # run_visualization_task 내에서 subtask_successes에 결과가 업데이트되고 실시간 로그 기록
        visualization_success = run_visualization_task(parameter, subtask_successes)
        
        # 3. 전체 태스크 성공 정의
        if visualization_success:
            final_task_success = True
            _log_message(parameter, "INFO", "✨ Task SUCCESS: All 5 subtasks completed successfully (RabbitMQ status noted).")
        else:
            final_task_success = False
            _log_message(parameter, "ERROR", "Task FAILURE: Visualization subtask failed (Validation Timeout or File Error).")
    else:
        final_task_success = False
        subtask_successes[VISUALIZATION_TASK] = "SKIPPED" 
        _write_result_log(parameter, False, {VISUALIZATION_TASK: "SKIPPED"}, "")

        failed_subtasks = [k for k, v in subtask_successes.items() if v == "FAILURE" and k != "rabbitmq"]
        
        if is_rabbitmq_failed and not is_exactly_once:
             failed_subtasks.append("rabbitmq")
             
        _log_message(parameter, "ERROR", f"Task FAILURE: Main subtasks failed. Failed: {', '.join(failed_subtasks)}")
        
    
    # 4. 최종 결과 요약 데이터 생성 및 파일 기록 (FINAL 로그)
    result_data = {}
    final_failure_reason = ""
    
    subtask_list = list(SUBTASKS_CONFIG.keys()) + [VISUALIZATION_TASK]
    
    for name in subtask_list:
        status = subtask_successes.get(name)
        
        # rabbitmq의 조건부 무시 상태 반영
        if name == "rabbitmq" and is_rabbitmq_failed and is_exactly_once:
             result_data[name] = "FAILURE (IGNORED)" 
        else:
            result_data[name] = status if status else "UNKNOWN"
        
    if not final_task_success:
        if not all_main_subtasks_succeeded:
            failed_tasks = [k for k, v in subtask_successes.items() if v == "FAILURE" and k != "rabbitmq"]
            if is_rabbitmq_failed and not is_exactly_once:
                 failed_tasks.append("rabbitmq")
            final_failure_reason = f"Main subtasks failure: {', '.join(failed_tasks)}"
        elif all_main_subtasks_succeeded and not visualization_success:
            final_failure_reason = "Visualization subtask failed (Validation Timeout/File Error)"
    
    # 최종 결과 파일 기록 (is_final=True)
    _write_result_log(parameter, final_task_success, result_data, final_failure_reason, is_final=True)


    # 4-4. 콘솔/일반 로그 출력 (기존 로직 유지)
    _log_message(parameter, "INFO", f"================================================")
    _log_message(parameter, "INFO", f"Task Completed. Overall Success: {final_task_success}")
    _log_message(parameter, "INFO", "--- Final Subtask Results ---")
    
    for name, status in result_data.items():
        _log_message(parameter, "INFO", f"  -> {name.capitalize()}: {status}")

    _log_message(parameter, "INFO", f"================================================\n")

    return final_task_success

# ----------------------------------------------------------------------
# --- 실행 예시 (Example Usage) ---
# ----------------------------------------------------------------------

if __name__ == "__main__":
    
    print("--- Environment Variables Loaded ---")
    print(f"Warmup Time: {BENCHMARK_WARMUP_MINUTES} min")
    print(f"Benchmark Duration: {BENCHMARK_DURATION_MINUTES} min")
    print("------------------------------------\n")
    
    param_filepath = os.path.join(PROJECT_ROOT, PARAM_FILE_NAME)
    benchmark_parameters = _load_parameters_from_file(param_filepath)
    
    if not benchmark_parameters:
        print(f"No parameters loaded from {PARAM_FILE_NAME}. Exiting.")
    else:
        print(f"Successfully loaded {len(benchmark_parameters)} parameters. Starting benchmark runs...")
        
        for idx, param in enumerate(benchmark_parameters):
            print(f"\n--- RUN {idx+1}/{len(benchmark_parameters)} ---")
            is_successful = run_benchmark_task(param)
            print(f"Final Result for Task {idx+1} ({param}): {'SUCCESS' if is_successful else 'FAILURE'}")