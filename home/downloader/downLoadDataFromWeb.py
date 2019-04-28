import urllib.request




with urllib.request.urlopen("https://aviation-edge.com/v2/public/airlineDatabase?key=28bcf4-b436f8") as response:
    data = response.read()
    print(data)
