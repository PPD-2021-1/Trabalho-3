# Trabalho 1.3 - DHT

## Requisitos

- Python 3
- Paho Mqtt para python 
- Conexão com a internet (Usamos o broken público)

## Execução

Abra o shell na pasta raíz do trabalho e execute os seguintes comandos:

```
$ pip3 install paho-mqtt
$ python3 main.py
```

## Comandos disponiveis

### Recupera uma chave da DHT
```
get:[key]
``` 

### Insere uma chave da DHT
```
put:[key]:[message]
```

## DHT

### Requisições

Canal: 'hash'

#### Inserir uma chave

Requisição

```json
    {
        "type": "put",
        "id": "[Um id qualquer para identificar a mensagem]",
        "key": "[Chave onde será gravado]",
        "value": "[Valor que deseja armazenar]"
    }
```

Resposta

```json
    {
        "id": "[Mesmo id da mensagem de requisição]",
        "type": "server_response",
        "status": "201"
    }
```

O status 201 indica que a chave foi criada

#### Consultar chave

Requisição

```json
    {
        "type": "get",
        "id": "[Um id qualquer para identificar a mensagem]",
        "key": "[Chave onde será lida]",
    }
```

Resposta

```json
    {
        "id": "[Mesmo id da mensagem de requisição]",
        "type": "server_response",
        "status": "200",
        "value": "[Valor recupera]"
    }
```

O status 200 indica que a valor foi recuperado e o 404 que a valor não foi encontrado.

#### Autores
- <a href="https://github.com/Pequem" target="_blank">Fernando Guerra</a>
- <a href="https://github.com/vitorchane" target="_blank">Vitor Chane</a>