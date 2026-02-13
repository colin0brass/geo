"""Tests for core progress primitives."""

from geo_core.progress import ProgressManager, get_progress_manager


def test_progress_manager_register_handler():
    manager = ProgressManager()

    class MockHandler:
        def on_location_start(self, *args):
            pass

        def on_year_start(self, *args):
            pass

        def on_year_complete(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    handler = MockHandler()
    manager.register_handler(handler)

    assert len(manager.handlers) == 1
    assert manager.handlers[0] is handler


def test_progress_manager_clear_handlers():
    manager = ProgressManager()

    class MockHandler:
        def on_location_start(self, *args):
            pass

        def on_year_start(self, *args):
            pass

        def on_year_complete(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    manager.register_handler(MockHandler())
    assert len(manager.handlers) == 1

    manager.clear_handlers()
    assert len(manager.handlers) == 0


def test_progress_manager_notify_location_start():
    manager = ProgressManager()

    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None

        def on_location_start(self, location_name, location_num, total_locations, total_years=1):
            self.called = True
            self.args = (location_name, location_num, total_locations, total_years)

        def on_year_start(self, *args):
            pass

        def on_year_complete(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    mock_handler = MockHandler()
    manager.register_handler(mock_handler)

    manager.notify_location_start("Test City", 1, 5)

    assert mock_handler.called
    assert mock_handler.args == ("Test City", 1, 5, 1)


def test_progress_manager_notify_year_start():
    manager = ProgressManager()

    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None

        def on_year_start(self, location_name, year, current_year, total_years):
            self.called = True
            self.args = (location_name, year, current_year, total_years)

        def on_location_start(self, *args):
            pass

        def on_year_complete(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    mock_handler = MockHandler()
    manager.register_handler(mock_handler)

    manager.notify_year_start("Test City", 2024, 1, 3)

    assert mock_handler.called
    assert mock_handler.args == ("Test City", 2024, 1, 3)


def test_progress_manager_notify_year_complete():
    manager = ProgressManager()

    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None

        def on_year_complete(self, location_name, year, current_year, total_years):
            self.called = True
            self.args = (location_name, year, current_year, total_years)

        def on_location_start(self, *args):
            pass

        def on_year_start(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    mock_handler = MockHandler()
    manager.register_handler(mock_handler)

    manager.notify_year_complete("Test City", 2024, 1, 3)

    assert mock_handler.called
    assert mock_handler.args == ("Test City", 2024, 1, 3)


def test_progress_manager_notify_location_complete():
    manager = ProgressManager()

    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None

        def on_location_complete(self, location_name):
            self.called = True
            self.args = (location_name,)

        def on_location_start(self, *args):
            pass

        def on_year_start(self, *args):
            pass

        def on_year_complete(self, *args):
            pass

    mock_handler = MockHandler()
    manager.register_handler(mock_handler)

    manager.notify_location_complete("Test City")

    assert mock_handler.called
    assert mock_handler.args == ("Test City",)


def test_progress_manager_multiple_handlers():
    manager = ProgressManager()

    class MockHandler:
        def __init__(self):
            self.call_count = 0

        def on_year_complete(self, *args):
            self.call_count += 1

        def on_location_start(self, *args):
            pass

        def on_year_start(self, *args):
            pass

        def on_location_complete(self, *args):
            pass

    handler1 = MockHandler()
    handler2 = MockHandler()

    manager.register_handler(handler1)
    manager.register_handler(handler2)

    manager.notify_year_complete("Test", 2024, 1, 1)

    assert handler1.call_count == 1
    assert handler2.call_count == 1


def test_get_progress_manager_singleton():
    manager1 = get_progress_manager()
    manager2 = get_progress_manager()

    assert manager1 is manager2
