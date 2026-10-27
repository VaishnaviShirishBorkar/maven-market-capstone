"""Package sanity test for maven_market module."""
import unittest


class TestPackageIntegrity(unittest.TestCase):
    def test_package_structure(self):
        """Verifies that the maven_market core namespace is loadable."""
        import maven_market
        self.assertIsNotNone(maven_market)


if __name__ == "__main__":
    unittest.main()
