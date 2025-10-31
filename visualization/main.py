import sys
from cpumemdisk_main import main as cpumemdisk_main
from network_main import main as network_main

def main():
    argv_len = len(sys.argv)
    if argv_len == 1:
        cpumemdisk_main()
    elif argv_len == 5:
        prod_count, part_count, cons_count, delivery_mode = sys.argv[1:]
        match delivery_mode:
            case 'L': delivery_mode = 'AT_LEAST_ONCE'
            case 'M': delivery_mode = 'AT_MOST_ONCE'
            case 'E': delivery_mode = 'EXACTLY_ONCE'
            case 'AT_LEAST_ONCE': delivery_mode = 'AT_LEAST_ONCE'
            case 'AT_MOST_ONCE': delivery_mode = 'AT_MOST_ONCE'
            case 'EXACTLY_ONCE': delivery_mode = 'EXACTLY_ONCE'
            case _: 
                print(f'invalid delivery mode input: {delivery_mode}')
                return

        network_main(prod_count, part_count, cons_count, delivery_mode)

if __name__ == '__main__':
    main()