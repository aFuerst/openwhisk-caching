# Run simple OW thing

`{ow-home}/tools/admin/wskadmin user create afuerst`
> "dacf4133-3c3a-42d5-956c-37200c35f427:eextGdxo1jX99Uh4ms6gnS760ExMJEG3jakg0DWJ1ldqJlukrQILpbSxEA92z3Kw"

wsk property set --auth "dacf4133-3c3a-42d5-956c-37200c35f427:eextGdxo1jX99Uh4ms6gnS760ExMJEG3jakg0DWJ1ldqJlukrQILpbSxEA92z3Kw"

ow@ow-ubu-invoc:~/openwhisk-caching/ansible$ cat ~/act.py 
def main(args):
   name = args.get("name", "stranger")
   greeting = "Hello " + name + "!"
   print(greeting)
   return {"greeting": greeting}


wsk -i action create test ~/act.py 
wsk -i action invoke test -p name HELLO
