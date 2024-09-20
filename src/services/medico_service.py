import json
import uuid
import boto3
import pickle
from aws_lambda_powertools import Logger

class MedicoService():
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.s3_client = boto3.client('s3')
        self.cognito_client = boto3.client('cognito-idp')
   
    def inserir_dados_medico(self, body):
        cpf = body.get('cpf', '')
        nome = body.get('nome', '')
        crm = body.get('crm', '')
        email = body.get('email', '')
        senha = body.get('senha', '')

        json_data = {
            'nome': nome,
            'cpf': cpf,
            'crm': crm,
            'email': email, 
            'senha': senha,
            'horarios': []
            }
        
        pickled_obj = pickle.dumps(json_data)

        self.s3_client.put_object(
            Bucket="bucket-medicos-fiap",
            Key=f"{cpf}.pkl",
            Body=pickled_obj
        )

        cognito_response = self.cognito_client.sign_up(
            ClientId="",
            Username=email,
            Password=senha
        )

        return 201

    def deletar_dados_medico(self, body: dict):
        cpf = body.get('cpf', '')
        email = body.get('email', '')
        
        self.s3_client.delete_object(Bucket="bucket-medicos-fiap", Key=f"{cpf}.pkl")

        response = self.cognito_client.admin_delete_user(
            UserPoolId="USER_POOL_ID",
            Username=email
        )

        self.logger.info(f"Dados do medico cpf {cpf} foram deletados.")
        return 200

    def cadastrar_horarios(self, body):
        cpf = body.get('cpf', '')
        hora = body.get('hora', '')
        horario = {
             'id': uuid.uuid4(),
             'hora': hora,
             'paciente': ""
             }
        response = self.s3_client.get_object(Bucket="bucket-medicos-fiap", Key=f"{cpf}.pkl")
        json_data = response['Body'].read().decode('utf-8')
        json_data['horarios'].append(horario)
        
        pickled_obj = pickle.dumps(json_data)

        self.s3_client.put_object(
            Bucket="bucket-medicos-fiap",
            Key=f"{cpf}.pkl",
            Body=pickled_obj
        )
        return 201

    def editar_horarios(self, body):
        cpf = body.get('cpf', '')
        id_horario = body.get('id', '')
        hora = body.get('hora', '')

        response = self.s3_client.get_object(Bucket="bucket-medicos-fiap", Key=f"{cpf}.pkl")
        json_data = response['Body'].read().decode('utf-8')
        
        for horario in json_data['horarios']:
                if horario['id'] == id_horario:
                    horario['hora'] = hora 
                    horario['paciente'] = "" 

        pickled_obj = pickle.dumps(json_data)

        self.s3_client.put_object(
            Bucket="bucket-medicos-fiap",
            Key=f"{cpf}.pkl",
            Body=pickled_obj
        )
        return 201

    def listar_medicos(self, body):
        arquivos = self.__listar_arquivos_s3("bucket-medicos-fiap")

        if not arquivos:
            print(f'Nenhum arquivo encontrado no bucket {"bucket-medicos-fiap"}.')
            return None

        lista_nomes = []
        for arquivo in arquivos:
            dados = self.__ler_arquivo_s3("bucket-medicos-fiap", arquivo)
            if dados is not None and 'nome' in dados:
                lista_nomes.append(dados['nome'])

        return json.loads(lista_nomes)

    def listar_horarios_medico(self, body):
        nome = body.get('nome', '')
        
        horarios = []
        arquivos = self.__listar_arquivos_s3("bucket-medicos-fiap")
        
        for arquivo in arquivos:
            dados = self.__ler_arquivo_s3("bucket-medicos-fiap", arquivo)
            if dados is not None and nome in dados:
                horarios = dados['horarios']

        return json.dumps(horarios)
    
    def __listar_arquivos_s3(self, bucket_name):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except Exception as e:
            print(f'Erro ao listar objetos do S3: {str(e)}')
            return None

    def __ler_arquivo_s3(self, bucket_name, arquivo_s3):
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=arquivo_s3)
            conteudo = response['Body'].read().decode('utf-8')
            return json.loads(conteudo)

        except Exception as e:
            print(f'Erro ao ler o arquivo {arquivo_s3} do S3: {str(e)}')
            return None