# orm1

Object-relational mapping library for PostgreSQL

<!-- ORM 소프트웨어의 기능
  * 영속: Persistence
  * 조회: Queries
  * 스키마 형상 관리
  * 트랜잭션 관리
* 영속
  * 객체 논리: 세션, 아이덴티티 맵
  * 엔티티 타입과 필드, 테이블과 칼럼 매핑
  * 키에 의한 획득
  * 저장: 생성 또는 업데이트
  * aggregate: 복잡한 객체 저장 및 획득
* 조회: 
  * SQL Object Model과 Raw 쿼리
    * SQLCode: Array<SQLText | SQLVar | SQLName>
    * 파싱 로직
  * Structured Query 
    * 집합 정의
    * 목록 조회 및 슬라이싱
    * 개수 세기
* Non-goal 1. Lazy loading and Relations
  * async/await and flow with/without IO. IO가 발생하는 경우 await을 붙여야 하므로, 동일한 코드로 IO 발생하는 실행흐름과 발생하지 않는 실행흐름을 커버 불가
  * 단순하고 타입 안전한 엔티티의 영속을 지원해야 하는데, 객체 안에 어느때에는 있고 어느때에는 없는 속성이 있으면 안 됨 (prefetch_related).
  * 읽기 속도가 느리다면 읽기 최적화를 하면 간단히 해결됨. 읽기 전용 모델이나 VIEW 도입, CQRS 접근방식 등이 이미 선택지로 열려 있음.
* Non-goal 2. Schema management
  * 스키마를 관리하기 위해서는 데이터베이스의 모든 것을 알아야 함.
  * 조회나 영속 API에 영향을 주지 않는 SEQUENCE, INDEX 등도 ORM의 범위에 들어오게 됨
  * 데이터베이스 메타데이터 추상화와 공급자 API 일반화가 겹치는 데서 복잡도 상승
  * ORM과 분리된 별도의 스키마 관리 도구가 복잡도 관리에 더 유리 -->

## Features

* Persistence
* Queries

## Get Started

```python
from uuid import UUID

from orm1 import mapped


@mapped()
class Purchase:
    id: UUID | None
    code: str
    user_id: UUID | None

```