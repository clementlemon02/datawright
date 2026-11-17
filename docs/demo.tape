# Records docs/demo.gif: the merge-request report on the shop example.
# Needs vhs (brew install vhs) and uv. From the repo root: vhs docs/demo.tape
Output docs/demo.gif
Set Shell "bash"
Set FontSize 15
Set Width 1300
Set Height 780
Set Padding 24
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"

Hide
Type "export PS1='$ ' GIT_PAGER=cat PAGER=cat && REPO=$PWD && rm -rf /tmp/shop-demo && cp -r examples/shop /tmp/shop-demo && cd /tmp/shop-demo && git init -q -b main && git add -A && git commit -qm base && uv run -q $REPO/skills/data-context/scripts/datawright.py init > /dev/null 2>&1 && git checkout -q -b fix-canceled-spelling && sed -i.bak 's/cancelled/canceled/' models/staging/stg_orders.sql && rm models/staging/stg_orders.sql.bak && git commit -qam 'Fix spelling in stg_orders' && alias dw=.data-context/dw && clear"
Enter
Sleep 8s
Show

Type "git diff main --stat"
Enter
Sleep 2s
Type "dw review --base main"
Enter
Sleep 10s
