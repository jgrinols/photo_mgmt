"""container module for TestExtractJsonObjects"""
from ....agent import utilities

class TestExtractJsonObjects:
    """Tests for the utilities.extract_json_objects"""
    def test_simple(self):
        """test for simple case of single json object embedded in other text"""
        test_data = """
            abc.:&#{

            &z({"foo":[1,4,6]})$

        """
        cnt = 0
        for res in utilities.extract_json_objects(test_data):
            cnt += 1
            assert cnt == 1
            assert res is not None
            assert "foo" in res
            assert len(res["foo"]) == 3
            assert res["foo"][0] == 1
            assert res["foo"][1] == 4
            assert res["foo"][2] == 6

    def test_multi(self):
        """tests extraction of multiple json objects embedded in other text"""
        test_data = """
            include an empty {} json object!
            and a multi line one with data:
            {
                "list": [
                    { "foo": "bar" },
                    { "baz": 8 }
                ]
            } and now something that kinda looks like json { user=db }...
            and a couple inline objects__: {"user":"db"}{"pw":"***"}
        """
        results = list(utilities.extract_json_objects(test_data))
        assert len(results) == 4
        if results[0]:
            # first one should be empty and not hit this
            assert False
        assert len(results[1]["list"]) == 2
        assert "user" in results[2]
        assert results[3]["pw"] == "***"
