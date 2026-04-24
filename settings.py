from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator, PostgresDsn
from pathlib import Path
from uuid import UUID

ENV_FILE_PATH = Path(__file__).parent / '.env'

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE_PATH),
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )

    TOPS_URL: str = Field(default='https://tops-stage.mos.ru', description='URL приложения')
    TOKEN: str = Field(default='', description='Авторизационный токен пользователя')
    TOPS_DATA_TABLE_ID: UUID = Field(
        description='UUID таблицы данных TOPS'
    )

    # --- База данных Датапровайдера ---
    DATAPROVIDER_HOST: str = Field(default='10.126.145.36', description='Хост базы данных')
    DATAPROVIDER_PORT: int = Field(default=5432, description='Порт базы данных', ge=1, le=65535)
    DATAPROVIDER_DATABASE: str = Field(default='postgres', description='Имя базы данных')
    DATAPROVIDER_USER: str = Field(default='postgres', description='Пользователь базы данных')
    DATAPROVIDER_PWD: str = Field(default='', description='Пароль базы данных')

    DATAPROVIDER_DB_URL: Optional[PostgresDsn] = Field(
        default=None,
        description='Полный URL для подключения к PostgreSQL'
    )

    @field_validator('DATAPROVIDER_DB_URL', mode='before')
    @classmethod
    def assemble_database_url(cls, v: Optional[str], info) -> str:
        if v:
            return v

        values = info.data
        return f"postgresql://{values.get('DATAPROVIDER_USER')}:{values.get('DATAPROVIDER_PWD')}@" \
               f"{values.get('DATAPROVIDER_HOST')}:{values.get('DATAPROVIDER_PORT')}/{values.get('DATAPROVIDER_DATABASE')}"

    DATAPROVIDER_API_PORT: int = Field(
        default=3000,
        description='Порт Data Provider API',
        ge=1,
    )

    DATAPROVIDER_API_URL: Optional[str] = Field(
        default=None,
        description='Полный URL для подключения к Data Provider API'
    )

    @field_validator('DATAPROVIDER_API_URL', mode='before')
    @classmethod
    def assemble_data_provider_url(cls, v: Optional[str], info) -> str:
        if v:
            return v

        values = info.data
        return f"http://{values.get('DATAPROVIDER_HOST')}:{values.get('DATAPROVIDER_API_PORT')}"

    TABLE_NAME_CHECK_UPDATE: str = Field(default='tops_checkupdate_pp', description='Таблица для проверки выполнения задач по обновлению данных в сервисе')

settings = Settings()