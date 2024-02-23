from asyncio import run as asyncio_run

from prefect import serve

from jobs import DEPLOYMENTS


async def main() -> None:
    print("启动工作流...")
    await serve(*DEPLOYMENTS, print_starting_message=False)  # type: ignore


if __name__ == "__main__":
    asyncio_run(main())
