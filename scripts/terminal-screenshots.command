#!/bin/bash
cd -- "$(dirname -- "$0")/.." || exit 1

printf 'Разверни окно Terminal, чтобы вывод помещался целиком.\n'
printf 'Будет 5 экранов с командами и сохранёнными журналами опытов.\n'
printf 'После каждого скрина нажимай Enter для следующего.\n'
read -r -p 'Нажми Enter, чтобы начать. '

for panel in 1 2 3 4 5; do
    clear
    printf '$ python3 scripts/terminal_evidence.py --panel %s\n\n' "$panel"
    python3 scripts/terminal_evidence.py --panel "$panel" || exit 1
    printf '\n'
    read -r -p 'Enter -> следующий экран '
done

clear
printf 'Все 5 экранов показаны.\n'
