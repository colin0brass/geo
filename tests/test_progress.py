"""Tests for progress reporting system."""

from io import StringIO
import sys

from progress import (
    ProgressManager,
    ConsoleProgressHandler,
    get_progress_manager,
)


def test_progress_manager_register_handler():
    """Test registering a handler."""
    manager = ProgressManager()
    handler = ConsoleProgressHandler()
    
    manager.register_handler(handler)
    
    assert len(manager.handlers) == 1
    assert manager.handlers[0] is handler


def test_progress_manager_clear_handlers():
    """Test clearing all handlers."""
    manager = ProgressManager()
    handler = ConsoleProgressHandler()
    
    manager.register_handler(handler)
    assert len(manager.handlers) == 1
    
    manager.clear_handlers()
    assert len(manager.handlers) == 0


def test_progress_manager_notify_location_start():
    """Test notifying location start."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None
        
        def on_location_start(self, location_name, location_num, total_locations):
            self.called = True
            self.args = (location_name, location_num, total_locations)
        
        def on_year_start(self, *args): pass
        def on_month_complete(self, *args): pass
        def on_year_complete(self, *args): pass
        def on_location_complete(self, *args): pass
    
    mock_handler = MockHandler()
    manager.register_handler(mock_handler)
    
    manager.notify_location_start("Test City", 1, 5)
    
    assert mock_handler.called
    assert mock_handler.args == ("Test City", 1, 5)


def test_progress_manager_notify_year_start():
    """Test notifying year start."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None
        
        def on_year_start(self, location_name, year, total_months):
            self.called = True
            self.args = (location_name, year, total_months)
        
        def on_location_start(self, *args): pass
        def on_month_complete(self, *args): pass
        def on_year_complete(self, *args): pass
        def on_location_complete(self, *args): pass
    
    mock_handler = MockHandler()
    manager.register_handler(mock_handler)
    
    manager.notify_year_start("Test City", 2024, 12)
    
    assert mock_handler.called
    assert mock_handler.args == ("Test City", 2024, 12)


def test_progress_manager_notify_month_complete():
    """Test notifying month complete."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None
        
        def on_month_complete(self, location_name, year, month_num, total_months):
            self.called = True
            self.args = (location_name, year, month_num, total_months)
        
        def on_location_start(self, *args): pass
        def on_year_start(self, *args): pass
        def on_year_complete(self, *args): pass
        def on_location_complete(self, *args): pass
    
    mock_handler = MockHandler()
    manager.register_handler(mock_handler)
    
    manager.notify_month_complete("Test City", 2024, 6, 12)
    
    assert mock_handler.called
    assert mock_handler.args == ("Test City", 2024, 6, 12)


def test_progress_manager_notify_year_complete():
    """Test notifying year complete."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None
        
        def on_year_complete(self, location_name, year):
            self.called = True
            self.args = (location_name, year)
        
        def on_location_start(self, *args): pass
        def on_year_start(self, *args): pass
        def on_month_complete(self, *args): pass
        def on_location_complete(self, *args): pass
    
    mock_handler = MockHandler()
    manager.register_handler(mock_handler)
    
    manager.notify_year_complete("Test City", 2024)
    
    assert mock_handler.called
    assert mock_handler.args == ("Test City", 2024)


def test_progress_manager_notify_location_complete():
    """Test notifying location complete."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.called = False
            self.args = None
        
        def on_location_complete(self, location_name):
            self.called = True
            self.args = (location_name,)
        
        def on_location_start(self, *args): pass
        def on_year_start(self, *args): pass
        def on_month_complete(self, *args): pass
        def on_year_complete(self, *args): pass
    
    mock_handler = MockHandler()
    manager.register_handler(mock_handler)
    
    manager.notify_location_complete("Test City")
    
    assert mock_handler.called
    assert mock_handler.args == ("Test City",)


def test_console_progress_handler_output(capsys):
    """Test console progress handler output."""
    handler = ConsoleProgressHandler()
    
    # Test month complete
    handler.on_month_complete("Austin, TX", 2024, 6, 12)
    captured = capsys.readouterr()
    assert "Austin, TX" in captured.out
    assert "2024" in captured.out
    assert "6/12 months" in captured.out
    assert "█" in captured.out or "░" in captured.out  # Progress bar characters
    
    # Test year complete
    handler.on_year_complete("Austin, TX", 2024)
    captured = capsys.readouterr()
    assert captured.out == "\n"  # Should just print a newline


def test_get_progress_manager_singleton():
    """Test that get_progress_manager returns the same instance."""
    manager1 = get_progress_manager()
    manager2 = get_progress_manager()
    
    assert manager1 is manager2


def test_progress_manager_multiple_handlers():
    """Test notifying multiple handlers."""
    manager = ProgressManager()
    
    class MockHandler:
        def __init__(self):
            self.call_count = 0
        
        def on_month_complete(self, *args):
            self.call_count += 1
        
        def on_location_start(self, *args): pass
        def on_year_start(self, *args): pass
        def on_year_complete(self, *args): pass
        def on_location_complete(self, *args): pass
    
    handler1 = MockHandler()
    handler2 = MockHandler()
    
    manager.register_handler(handler1)
    manager.register_handler(handler2)
    
    manager.notify_month_complete("Test", 2024, 1, 12)
    
    assert handler1.call_count == 1
    assert handler2.call_count == 1
