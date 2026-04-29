def test_frontend_company_queue_registered() -> None:
    """Ensure the frontend company create queue is active and not pending by
    inspecting the registry source rather than importing the package (avoids
    heavy imports in CI environments)."""
    import io
    from pathlib import Path

    registry_path = Path("src/handlers/_registry.py")
    content = registry_path.read_text(encoding="utf-8")

    assert "crm.frontend.company.created" in content
    assert '"frontend.company.created": "user.topic"' not in content
